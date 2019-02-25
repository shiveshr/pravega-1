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
import io.pravega.controller.util.RetryHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class PravegaTablesStoreHelper {
    private static final int NUM_OF_TRIES = 5;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, ScheduledExecutorService executor) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;
    }

    public CompletableFuture<Void> createTable(String scope, String tableName) {
        return Futures.toVoid(runOnExecutorWithExceptionHandling(() -> segmentHelper.createTableSegment(scope, tableName, RequestTag.NON_EXISTENT_ID),
                "create table: " + scope + "/" + tableName))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        log.warn("create table %s/%s threw exception", scope, tableName, e);
                    } else {
                        log.debug("table %s/%s created successfully", scope, tableName);
                    }
                });
    }
    
    public CompletableFuture<Void> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        return runOnExecutorWithExceptionHandling(
                () -> Futures.toVoid(segmentHelper.deleteTableSegment(scope, tableName, mustBeEmpty, RequestTag.NON_EXISTENT_ID)),
                "delete table: " + scope + "/" + tableName);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, @NonNull byte[] value) {
        List<TableEntry<byte[], byte[]>> entries = new ArrayList<>();
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), KeyVersion.NOT_EXISTS), value);
        entries.add(entry);
        String errorMessage = "addNewEntry: key:" + key + " table: " + scope + "/" + tableName;
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID),
                errorMessage)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StoreException.WriteConflictException) {
                        throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage);
                    } else {
                        log.debug("add new entry %s to %s/%s threw exception %s %s", key, scope, tableName, unwrap.getClass(), unwrap.getMessage());
                        throw new CompletionException(e);
                    }
                })
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Version> addNewEntryIfAbsent(String scope, String tableName, String key, @NonNull byte[] value) {
        // if entry exists, we will get write conflict in attempting to create it again. 
        return Futures.exceptionallyExpecting(addNewEntry(scope, tableName, key, value),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, null);
    }

    public CompletableFuture<Version> updateEntry(String scope, String tableName, String key, Data value) {
        List<TableEntry<byte[], byte[]>> entries = new ArrayList<>();
        KeyVersionImpl version = value.getVersion() == null ? null :
                new KeyVersionImpl(value.getVersion().asLongVersion().getLongValue());
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), version), value.getData());
        entries.add(entry);
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID),
                "updateEntry: key:" + key + " table: " + scope + "/" + tableName)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new ArrayList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTable(scope, tableName, keys, RequestTag.NON_EXISTENT_ID),
                "get entry: key:" + key + " table: " + scope + "/" + tableName)
                .thenApply(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                });
    }

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new ArrayList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.removeTableKeys(scope, tableName, keys, 0L),
                "remove entry: key:" + key + " table: " + scope + "/" + tableName);
    }

    public CompletableFuture<Void> removeEntries(String scope, String tableName, List<String> key) {
        List<TableKey<byte[]>> keys = key.stream().map(x -> new TableKeyImpl<>(x.getBytes(), null)).collect(Collectors.toList());
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.removeTableKeys(scope, tableName, keys, 0L),
                "remove entries: keys:" + keys + " table: " + scope + "/" + tableName);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String scope, String tableName, ByteBuf continuationToken, int limit) {
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTableKeys(scope, tableName, limit, 
                IteratorState.fromBytes(continuationToken), 0L)
                .thenApply(result -> {
                    List<String> items = result.getItems().stream().map(x -> new String(x.getKey())).collect(Collectors.toList());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }), "get keys paginated for table:" + scope + "/" + tableName);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<Pair<String, Data>>>> getEntriesPaginated(String scope, String tableName, 
                                                                                               ByteBuf continuationToken, int limit) {
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTableEntries(scope, tableName, limit, 
                IteratorState.fromBytes(continuationToken), 0L)
                .thenApply(result -> {
                    List<Pair<String, Data>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey());
                        Data value = new Data(x.getValue(), new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                        return new ImmutablePair<>(key, value);
                    }).collect(Collectors.toList());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }), "get entries paginated for table:" + scope + "/" + tableName);
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(scope, tableName, token, 1000)
                .thenApply(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue())),
                IteratorState.EMPTY.toBytes());
    }

    public AsyncIterator<Pair<String, Data>> getAllEntries(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(scope, tableName, token, 100)
                .thenApply(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue())),
                IteratorState.EMPTY.toBytes());
    }

    private <T> CompletableFuture<T> translateException(CompletableFuture<T> future, String errorMessage) {
        return future.exceptionally(e -> {
            Throwable cause = Exceptions.unwrap(e);
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
            } else {
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    private <T> CompletableFuture<T> runOnExecutorWithExceptionHandling(Supplier<CompletableFuture<T>> futureSupplier, String errorMessage) {
        return RetryHelper.withRetriesAsync(() -> translateException(futureSupplier.get(), errorMessage), 
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException, NUM_OF_TRIES, executor);
    }
}
