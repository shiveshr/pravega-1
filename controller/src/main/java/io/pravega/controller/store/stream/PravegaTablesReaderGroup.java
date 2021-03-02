/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_UUID_FUNCTION;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * PravegaTables ReaderGroup.
 * This creates a top level metadata table for each readergroup.
 * All metadata records are stored in this metadata table.
 *
 * Each Reader Group is protected against recreation of another Reader Group with same name by attaching a UUID to the name.
 */
@Slf4j
class PravegaTablesReaderGroup extends AbstractReaderGroup {
    public static final String SEPARATOR = ".#.";
    private static final String READER_GROUPS_TABLE_IDENTIFIER = "_readergroups";
    private static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";
    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String STATE_KEY = "state";

    private final PravegaTablesStoreHelper storeHelper;
    private final Function<Boolean, CompletableFuture<String>> readerGroupsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final long operationStartTime;
    private final long requestId;

    @VisibleForTesting
    PravegaTablesReaderGroup(final String scopeName, final String rgName, PravegaTablesStoreHelper storeHelper,
                             Function<Boolean, CompletableFuture<String>> rgInScopeTableNameSupplier,
                             ScheduledExecutorService executor, long requestId) {
        super(scopeName, rgName);
        this.storeHelper = storeHelper;
        this.readerGroupsInScopeTableNameSupplier = rgInScopeTableNameSupplier;
        this.operationStartTime = System.currentTimeMillis();
        this.requestId = requestId;
        this.idRef = new AtomicReference<>(null);
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // first get the scope id from the cache.
            // if the cache does not contain scope id then we load it from the supplier. 
            // if cache contains the scope id then we load the streamid. if not found, we load the whole shit
            return Futures.exceptionallyComposeExpecting(
                    readerGroupsInScopeTableNameSupplier.apply(false).thenCompose(streamsInScopeTable ->
                            storeHelper.getCachedOrLoad(streamsInScopeTable, getName(),
                                    BYTES_TO_UUID_FUNCTION, operationStartTime, requestId)),
                    e -> Exceptions.unwrap(e) instanceof StoreException.DataContainerNotFoundException,
                    () -> readerGroupsInScopeTableNameSupplier.apply(true).thenCompose(streamsInScopeTable ->
                            storeHelper.getCachedOrLoad(streamsInScopeTable, getName(),
                                    BYTES_TO_UUID_FUNCTION, operationStartTime, requestId)))
                    .thenComposeAsync(data -> {
                        idRef.compareAndSet(null, data.getObject().toString());
                        return getId();
                    });
        }
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), READER_GROUPS_TABLE_IDENTIFIER, getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    CompletableFuture<Void> createMetadataTables() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            return storeHelper.createTable(metadataTable, requestId)
                    .thenAccept(v -> log.debug("reader group {}/{} metadata table {} created", getScope(), getName(), metadataTable));
        });
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        return Futures.toVoid(getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY,
                        creationTime, PravegaTablesStoreHelper.LONG_TO_BYTES_FUNCTION, requestId)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig configuration) {
        ReaderGroupConfigRecord configRecord = ReaderGroupConfigRecord.update(configuration, 0L, false);
        return Futures.toVoid(getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY,
                        configRecord, ReaderGroupConfigRecord::toBytes, requestId)));
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent() {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY,
                        ReaderGroupStateRecord.builder().state(ReaderGroupState.CREATING).build(), 
                        ReaderGroupStateRecord::toBytes, requestId)));

    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<ReaderGroupStateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY, ReaderGroupStateRecord::toBytes, 
                        state.getObject(), state.getVersion(), requestId));
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, STATE_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, STATE_KEY, ReaderGroupStateRecord::fromBytes, 
                            ignoreCached ? operationStartTime : 0L, requestId);
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, CONFIGURATION_KEY, ReaderGroupConfigRecord::fromBytes,
                            ignoreCached ? operationStartTime : 0L, requestId);
                });
    }

    @Override
    public CompletableFuture<Void> delete() {
        return getId().thenCompose(id -> storeHelper.deleteTable(getMetadataTableName(id), false, requestId)
        .thenCompose(v -> {
            this.idRef.set(null);
            return CompletableFuture.completedFuture(null);
        }));
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<ReaderGroupConfigRecord> configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CONFIGURATION_KEY, ReaderGroupConfigRecord::toBytes, 
                        configuration.getObject(), configuration.getVersion(), requestId));
    }
}
