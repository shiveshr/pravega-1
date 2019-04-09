/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.netty.buffer.ByteBuf;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.util.RetryHelper;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

@Slf4j
public class PravegaTablesStoreHelper {
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    private final AtomicReference<String> authToken;
    private final AuthHelper authHelper;
    
    @lombok.Data
    @EqualsAndHashCode
    private class TableCacheKey<T> implements Cache.CacheKey {
        private final String scope;
        private final String table;
        private final String key;

        private final Function<byte[], T> fromBytesFunc;
    }
    
    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, AuthHelper authHelper, ScheduledExecutorService executor) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;

        cache = new Cache(x -> {
            TableCacheKey<?> entryKey = (TableCacheKey<?>) x;

            // Since there are be multiple tables, we will cache `table+key` in our cache
            return getEntry(entryKey.getScope(), entryKey.getTable(), entryKey.getKey(), entryKey.fromBytesFunc)
                                   .thenApply(v -> new VersionedMetadata<>(v.getObject(), v.getVersion()));
        });
        this.authHelper = authHelper;
        this.authToken = new AtomicReference<>(authHelper.retrieveMasterToken());
    }

    public <T> CompletableFuture<VersionedMetadata<T>> getCachedData(String scope, String table, String key, Function<byte[], T> fromBytes) {
        return cache.getCachedData(new TableCacheKey<>(scope, table, key, fromBytes))
                    .thenApply(this::getVersionedMetadata);
    }

    @SuppressWarnings("unchecked")
    private <T> VersionedMetadata<T> getVersionedMetadata(VersionedMetadata v) {
        // Since cache is untyped and holds all types of deserialized objects, we typecast it to the requested object type
        // based on the type in caller's supplied Deserialization function. 
        return new VersionedMetadata<>((T) v.getObject(), v.getVersion());
    }

    public void invalidateCache(String scope, String table, String key) {
        cache.invalidateCache(new TableCacheKey<>(scope, table, key, x -> null));
    }

    public CompletableFuture<Void> createTable(String scope, String tableName) {
        log.debug("create table called for table: {}/{}", scope, tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(scope, tableName, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("create table: %s/%s", scope, tableName)))
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        log.warn("create table {}/{} threw exception", scope, tableName, e);
                    } else {
                        log.debug("table {}/{} created successfully", scope, tableName);
                    }
                }, executor);
    }
    
    public CompletableFuture<Void> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        log.debug("delete table called for table: {}/{}", scope, tableName);
        return expectingDataNotFound(withRetries(() -> segmentHelper.deleteTableSegment(
                scope, tableName, mustBeEmpty, authToken.get(), RequestTag.NON_EXISTENT_ID), 
                () -> String.format("delete table: %s/%s", scope, tableName)), null)
                .thenAcceptAsync(v -> log.debug("table {}/{} deleted successfully", scope, tableName), executor);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, @NonNull byte[] value) {
        log.debug("addNewEntry called for : {}/{} key : {}", scope, tableName, key);

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), KeyVersion.NOT_EXISTS), value));
        Supplier<String> errorMessage = () -> String.format("addNewEntry: key: %s table: %s/%s", key, scope, tableName);
        return withRetries(() -> segmentHelper.updateTableEntries(scope, tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                errorMessage)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StoreException.WriteConflictException) {
                        throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                    } else {
                        log.debug("add new entry {} to {}/{} threw exception {} {}", key, scope, tableName, unwrap.getClass(), unwrap.getMessage());
                        throw new CompletionException(e);
                    }
                })
                .thenApplyAsync(x -> {
                    KeyVersion first = x.get(0);
                    log.debug("entry for key {} added to table {}/{} with version {}", key, scope, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
    }

    public CompletableFuture<Version> addNewEntryIfAbsent(String scope, String tableName, String key, @NonNull byte[] value) {
        // if entry exists, we will get write conflict in attempting to create it again. 
        return expectingDataExists(addNewEntry(scope, tableName, key, value), null);
    }

    public CompletableFuture<Void> addNewEntriesIfAbsent(String scope, String tableName, Map<String, byte[]> toAdd) {
        // What happens if we get data exists for one such record? though pravegatablestream, only creates these keys in batch
        // so ignoring the exception of data exists is safe. But what happens otherwise?
        List<TableEntry<byte[], byte[]>> entries = toAdd.entrySet().stream().map(x ->
                new TableEntryImpl<>(new TableKeyImpl<>(x.getKey().getBytes(Charsets.UTF_8), KeyVersion.NOT_EXISTS), x.getValue()))
                                                        .collect(Collectors.toList());
        Supplier<String> errorMessage = () -> String.format("addNewEntriesIfAbsent: table: %s/%s", scope, tableName);
        return expectingDataExists(withRetries(() -> segmentHelper.updateTableEntries(scope, tableName, entries, authToken.get(), 
                        RequestTag.NON_EXISTENT_ID), errorMessage)
                .handle((r, e) -> {
                    if (e != null) {
                        Throwable unwrap = Exceptions.unwrap(e);
                        if (unwrap instanceof StoreException.WriteConflictException) {
                            throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                        } else {
                            log.debug("add new entries to {}/{} threw exception {} {}", scope, tableName, unwrap.getClass(), unwrap.getMessage());
                            throw new CompletionException(e);
                        }
                    } else {
                        log.debug("entries added to table {}/{}", scope, tableName);
                        return null;
                    }
                }), null);
    }
    
    public CompletableFuture<Version> updateEntry(String scope, String tableName, String key, byte[] data, Version v) {
        log.debug("updateEntry entry called for : {}/{} key : {} version {}", scope, tableName, key, v.asLongVersion().getLongValue());

        KeyVersionImpl version = new KeyVersionImpl(v.asLongVersion().getLongValue());

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), version), data));
        return withRetries(() -> segmentHelper.updateTableEntries(scope, tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("updateEntry: key: %s table: %s/%s", key, scope, tableName))
                .thenApplyAsync(x -> {
                    KeyVersion first = x.get(0);
                    log.debug("entry for key {} updated to table {}/{} with new version {}", key, scope, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
    }

    public <T> CompletableFuture<VersionedMetadata<T>> getEntry(String scope, String tableName, String key, Function<byte[], T> fromBytes) {
        log.debug("get entry called for : {}/{} key : {}", scope, tableName, key);
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        CompletableFuture<VersionedMetadata<T>> result = new CompletableFuture<>();
        String message = "get entry: key: %s table: %s/%s";
        withRetries(() -> segmentHelper.readTable(scope, tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, key, scope, tableName))
                .thenApplyAsync(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    if (first.getKey().getVersion().equals(KeyVersion.NOT_EXISTS)) {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, String.format(message, key, scope, tableName));   
                    } else {
                        log.debug("returning entry for : {}/{} key : {} with version {}", scope, tableName, key,
                                first.getKey().getVersion().getSegmentVersion());

                        T deserialized = fromBytes.apply(first.getValue());

                        return new VersionedMetadata<>(deserialized, new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
                   if (e != null) {
                       result.completeExceptionally(e);
                   } else {
                       result.complete(r);
                   }
                }, executor);
        return result;
    }

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        log.debug("remove entry called for : {}/{} key : {}", scope, tableName, key);

        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                scope, tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID), 
                () -> String.format("remove entry: key: %s table: %s/%s", key, scope, tableName)), null)
                .thenAcceptAsync(v -> log.debug("entry for key {} removed from table {}/{}", key, scope, tableName), executor);
    }
    
    public CompletableFuture<Void> removeEntries(String scope, String tableName, Collection<String> keys) {
        log.debug("remove entry called for : {}/{} keys : {}", scope, tableName, keys);

        List<TableKey<byte[]>> listOfKeys = keys.stream().map(x -> new TableKeyImpl<>(x.getBytes(Charsets.UTF_8), null)).collect(Collectors.toList());
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                scope, tableName, listOfKeys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entries: keys: %s table: %s/%s", keys.toString(), scope, tableName)), null)
                .thenAcceptAsync(v -> log.debug("entry for keys {} removed from table {}/{}", keys, scope, tableName), executor);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String scope, String tableName, ByteBuf continuationToken, int limit) {
        log.debug("get keys paginated called for : {}/{}", scope, tableName);

        return withRetries(() -> 
                segmentHelper.readTableKeys(scope, tableName, limit, IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                        () -> String.format("get keys paginated for table: %s/%s", scope, tableName))
                             .thenApplyAsync(result -> {
                                 List<String> items = result.getItems().stream().map(x -> new String(x.getKey(), Charsets.UTF_8))
                                                            .collect(Collectors.toList());
                                 log.debug("get keys paginated on table {}/{} returned items {}", scope, tableName, items);
                                 return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                             }, executor);
    }

    public <T> CompletableFuture<Map.Entry<ByteBuf, List<Pair<String, VersionedMetadata<T>>>>> getEntriesPaginated(
            String scope, String tableName, ByteBuf continuationToken, int limit, 
            Function<byte[], T> fromBytes) {
        log.debug("get entries paginated called for : {}/{}", scope, tableName);
        return withRetries(() -> segmentHelper.readTableEntries(scope, tableName, limit,
                IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s/%s", scope, tableName))
                .thenApplyAsync(result -> {
                    List<Pair<String, VersionedMetadata<T>>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey(), Charsets.UTF_8);
                        T deserialized = fromBytes.apply(x.getValue());
                        VersionedMetadata<T> value = new VersionedMetadata<>(deserialized, new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                        return new ImmutablePair<>(key, value);
                    }).collect(Collectors.toList());
                    log.debug("get keys paginated on table {}/{} returned number of items {}", scope, tableName, items.size());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    public <K, V> CompletableFuture<Map<K, V>> getEntriesWithFilter(
            String scope, String table, Function<String, K> fromStringKey,
            Function<byte[], V> fromBytesValue, BiFunction<K, V, Boolean> filter, int limit) {
        Map<K, V> result = new ConcurrentHashMap<>();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        AtomicReference<ByteBuf> token = new AtomicReference<>(IteratorState.EMPTY.toBytes());

        return Futures.exceptionallyExpecting(
                Futures.loop(canContinue::get,
                        () -> getEntriesPaginated(scope, table, token.get(), limit, fromBytesValue)
                                .thenAccept(v -> {
                                    // we exit if we have either received `limit` number of entries
                                    List<Pair<String, VersionedMetadata<V>>> pair = v.getValue();
                                    for (Pair<String, VersionedMetadata<V>> val : pair) {
                                        K key = fromStringKey.apply(val.getKey());
                                        V value = val.getValue().getObject();
                                        if (filter.apply(key, value)) {
                                            result.put(key, value);
                                            if (result.size() == limit) {
                                                break;
                                            }
                                        }
                                    }
                                    // if we get less than the requested number, then we will exit the loop. 
                                    // otherwise if we have collected all the desired results
                                    canContinue.set(!(v.getValue().size() < limit || result.size() >= limit));
                                    token.get().release();
                                    if (canContinue.get()) {
                                        // set next continuation token
                                        token.set(v.getKey());
                                    }
                                }), executor)
                       .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap());
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(scope, tableName, token, 1000)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorState.EMPTY.toBytes());
    }

    public <T> AsyncIterator<Pair<String, VersionedMetadata<T>>> getAllEntries(String scope, String tableName, Function<byte[], T> fromBytes) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(scope, tableName, token, 1000, fromBytes)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorState.EMPTY.toBytes());
    }

    public <T> CompletableFuture<T> expectingDataNotFound(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, toReturn);
    }

    public <T> CompletableFuture<T> expectingDataExists(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, toReturn);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future, Supplier<String> errorMessageSupplier) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                    case UnknownHost:
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case PreconditionFailed:
                        toThrow = StoreException.create(StoreException.Type.ILLEGAL_STATE, wcfe, errorMessage);
                        break;
                    case AuthFailed:
                        authToken.set(authHelper.retrieveMasterToken());
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableSegmentNotEmpty:
                        toThrow = StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, wcfe, errorMessage);
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreException.create(StoreException.Type.WRITE_CONFLICT, wcfe, errorMessage);
                        break;
                    default:
                        toThrow = StoreException.create(StoreException.Type.UNKNOWN, wcfe, errorMessage);
                }
            } else if (cause instanceof HostStoreException) {
                log.warn("Host Store exception {}", cause.getMessage());
                toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, cause, errorMessage);
            } else if (cause instanceof ArrayIndexOutOfBoundsException) {
                // todo: temporary hack added to work around concurrent write and get issue
                log.warn("received array out of bounds", cause.getMessage());
                toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, cause, errorMessage);
            } else {
                log.warn("exception of unknown type thrown {} ", errorMessage, cause);
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    /*
     * We dont want to do indefinite retries because for controller's graceful shutting down, it waits on grpc service to
     * be terminated which in turn waits on all outstanding grpc calls to complete. And the store may stall the calls if
     * there is indefinite retries. Restricting it to 12 retries gives us ~60 seconds worth of wait on the upper side.
     * Also, note that the call can fail because hostContainerMap has not been updated or it can fail because it cannot
     * talk to segment store. Both these are translated to ConnectionErrors and are retried. All other exceptions
     * are thrown back
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage),
                e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    return unwrap instanceof StoreException.StoreConnectionException;
                }, NUM_OF_RETRIES, executor);
    }
}
