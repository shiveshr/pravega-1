/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

@Slf4j
public abstract class AbstractKVTableMetadataStore implements KVTableMetadataStore {
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;

    @Getter
    private final HostIndex hostTaskIndex;

    protected AbstractKVTableMetadataStore(HostIndex hostTaskIndex) {
        this.hostTaskIndex = hostTaskIndex;
    }

    protected Scope getScope(final String scopeName, long requestId) {
        return newScope(scopeName, requestId);
    }

    public Scope getScope(final String scopeName, final OperationContext context) {
        if (context != null) {
            if (context instanceof KVTOperationContext) {
                return ((KVTOperationContext) context).getScope();
            } else {
                return newScope(scopeName, context.getRequestId());
            }
        } else {
            // TODO: shivesh
            long requestId = 0L;
            return newScope(scopeName, requestId);
        }
    }

    abstract KeyValueTable newKeyValueTable(final String scope, final String kvTableName, long requestId);

    @Override
    public KVTOperationContext createContext(String scopeName, String name, long requestId) {
        Scope scope = newScope(scopeName, requestId);
        KeyValueTable kvTable = newKeyValueTable(scopeName, name, requestId);
        return new KVTOperationContext(scope, kvTable, requestId);
    }

    public KeyValueTable getKVTable(String scope, final String name, OperationContext context) {
        KeyValueTable kvt;
        if (context != null) {
            if (context instanceof KVTOperationContext) {
                kvt = ((KVTOperationContext) context).getKvTable();
                assert kvt.getScopeName().equals(scope);
                assert kvt.getName().equals(name);
            } else {
                kvt = newKeyValueTable(scope, name, context.getRequestId());
            }
        } else {
            // todo: shivesh
            kvt = newKeyValueTable(scope, name, 0L);
        }
        return kvt;
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scope,
                                                                final String name,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        long requestId = context != null ? context.getRequestId() : 0L; // TODO: shivesh

        return Futures.completeOn(checkScopeExists(scope, context, executor)
                .thenCompose(exists -> {
                    if (exists) {
                        // Create kvtable may fail if scope is deleted as we attempt to create the table under scope.
                        return getSafeStartingSegmentNumberFor(scope, name, context, executor)
                                .thenCompose(startingSegmentNumber -> getKVTable(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber));
                    } else {
                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                    }
                }), executor);
    }

    String getScopedKVTName(String scope, String name) {
        return String.format("%s/%s", scope, name);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final OperationContext context,
                                                   final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getCreationTime(), executor);
    }

    @Override
    public CompletableFuture<Void> setState(final String scope, final String name,
                                            final KVTableState state, final OperationContext context,
                                            final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).updateState(state), executor);
    }

    @Override
    public CompletableFuture<KVTableState> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext context,
                                             final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getState(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                                            final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                                            final OperationContext context,
                                                                            final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).updateVersionedState(previous, state), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getVersionedState(), executor);
    }

    @Override
    public CompletableFuture<List<KVTSegmentRecord>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getActiveSegments(), executor);
    }

    @Override
    public CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope,
                                                                          final String name,
                                                                          final OperationContext context, 
                                                                          final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getConfiguration(), executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final String scopeName, final String continuationToken,
                                                                            final int limit, final OperationContext context,
                                                                            final Executor executor) {
        return getScope(scopeName, context).listKeyValueTables(limit, continuationToken);
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final OperationContext context,
                                                         final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getAllSegmentIds(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteKeyValueTable(final String scope,
                                                       final String name,
                                                       final OperationContext context,
                                                       final Executor executor) {
        OperationContext kvtContext = context != null ? context : createContext(scope, name, 0L);
        return Futures.exceptionallyExpecting(CompletableFuture.completedFuture(getKVTable(scope, name, kvtContext))
                .thenCompose(kvt -> kvt.getActiveEpochRecord(true))
                            .thenApply(epoch -> epoch.getSegments().stream().map(KVTSegmentRecord::getSegmentNumber)
                                .reduce(Integer::max).get())
                        .thenCompose(lastActiveSegment -> recordLastKVTableSegment(scope, name, lastActiveSegment, context, executor)),
                DATA_NOT_FOUND_PREDICATE, null)
                .thenCompose(v -> deleteFromScope(scope, name, context, executor))
                .thenCompose(v -> Futures.completeOn(getKVTable(scope, name, kvtContext).delete(), executor));
    }

    /**
     * This method stores the last active segment for a stream upon its deletion. Persistently storing this value is
     * necessary in the case of a stream re-creation for picking an appropriate starting segment number.
     *
     * @param scope scope
     * @param kvtable kvtable
     * @param lastActiveSegment segment with highest number for a kvtable
     * @param context context
     * @param executor executor
     * @return CompletableFuture which indicates the task completion related to persistently store lastActiveSegment.
     */
    abstract CompletableFuture<Void> recordLastKVTableSegment(final String scope, final String kvtable, int lastActiveSegment,
                                                              final OperationContext context, final Executor executor);

    /**
     * This method retrieves a safe base segment number from which a stream's segment ids may start. In the case of a
     * new stream, this method will return 0 as a starting segment number (default). In the case that a stream with the
     * same name has been recently deleted, this method will provide as a safe starting segment number the last segment
     * number of the previously deleted stream + 1. This will avoid potential segment naming collisions with segments
     * being asynchronously deleted from the segment store.
     *
     * @param scopeName scope
     * @param kvtName KeyValueTable name
     * @return CompletableFuture with a safe starting segment number for this stream.
     */
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName, 
                                                                        final OperationContext context, final Executor executor);

    public abstract CompletableFuture<Boolean> checkScopeExists(final String scope, final OperationContext context, 
                                                                final Executor executor);

    public abstract CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                                  final String kvtName,
                                                                  final byte[] id,
                                                                  final OperationContext context, 
                                                                  final Executor executor);

    abstract CompletableFuture<Void> deleteFromScope(final String scope,
                                                            final String name,
                                                            final OperationContext context,
                                                            final Executor executor);
}
