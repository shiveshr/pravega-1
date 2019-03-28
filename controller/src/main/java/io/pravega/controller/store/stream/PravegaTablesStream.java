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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.COMPLETED_TRANSACTIONS_BATCHES_TABLE;

@Slf4j
/**
 * Pravega Table Stream. 
 * This creates two top level tables per stream - metadataTable, epochsWithTransactionsTable. 
 * All metadata records are stored in metadata table. 
 * EpochsWithTransactions is a top level table for storing epochs where any transaction was created. 
 *
 * This class is coded for transaction ids that follow the scheme that msb 32 bits represent epoch
 */
class PravegaTablesStream extends PersistentStreamBase {
    private static final String STREAM_TABLE_PREFIX = "Table" + SEPARATOR + "%s" + SEPARATOR; // stream name
    private static final String METADATA_TABLE = STREAM_TABLE_PREFIX + "metadata" + SEPARATOR + "%s";
    private static final String TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "transactionsTable" + SEPARATOR + "%s";

    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String TRUNCATION_KEY = "truncation";
    private static final String STATE_KEY = "state";
    private static final String EPOCH_TRANSITION_KEY = "epochTransition";
    private static final String RETENTION_SET_KEY = "retention";
    private static final String RETENTION_STREAM_CUT_RECORD_KEY_FORMAT = "retentionCuts-%s"; // stream cut reference
    private static final String CURRENT_EPOCH_KEY = "currentEpochRecord";
    private static final String EPOCH_RECORD_KEY_FORMAT = "epochRecord-%d";
    private static final String HISTORY_TIMESERES_CHUNK_FORMAT = "historyTimeSeriesChunk-%d";
    private static final String SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT = "segmentsSealedSizeMapShard-%d";
    private static final String SEGMENT_SEALED_EPOCH_KEY_FORMAT = "segmentSealedEpochPath-%d"; // segment id
    private static final String COMMITTING_TRANSACTIONS_RECORD_KEY = "committingTxns";
    private static final String SEGMENT_MARKER_PATH_FORMAT = "markers-%d";
    private static final String WAITING_REQUEST_PROCESSOR_PATH = "waitingRequestProcessor";

    // completed transactions key
    private static final String STREAM_KEY_PREFIX = "Key" + SEPARATOR + "%s" + SEPARATOR + "%s" + SEPARATOR; // scoped stream name
    private static final String COMPLETED_TRANSACTIONS_KEY_FORMAT = STREAM_KEY_PREFIX + "/%s";

    private final PravegaTablesStoreHelper storeHelper;

    private final Supplier<Integer> currentBatchSupplier;
    private final Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final ScheduledExecutorService executor;
    
    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier,
                        ScheduledExecutorService executor) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier, executor);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier, ScheduledExecutorService executor) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;

        this.currentBatchSupplier = currentBatchSupplier;
        this.streamsInScopeTableNameSupplier = streamsInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return streamsInScopeTableNameSupplier.get()
                                                  .thenCompose(streamsInScopeTable -> 
                                                          storeHelper.getEntry(getScope(), streamsInScopeTable, getName(), 
                                                          x -> BitConverter.readUUID(x, 0)))
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
        return String.format(METADATA_TABLE, getName(), id);
    }

    private CompletableFuture<String> getTransactionsTable() {
        return getId().thenApply(this::getTransactionsTableName);
    }

    private String getTransactionsTableName(String id) {
        return String.format(TRANSACTIONS_TABLE, getName(), id);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        AtomicInteger count = new AtomicInteger(0);
        
        AtomicReference<ByteBuf> token = new AtomicReference<>(Unpooled.wrappedBuffer(new byte[0]));
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return getTransactionsTable()
                .thenCompose(table -> Futures.loop(canContinue::get, 
                        () -> storeHelper.getKeysPaginated(getScope(), table, token.get(), 1000)
                                                 .thenAccept(v -> {
                                                     canContinue.set(!v.getValue().isEmpty());
                                                     count.addAndGet(v.getValue().size());
                                                     token.set(v.getKey());
                                                 }), executor))
                .thenApply(v -> count.get());
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        String scope = getScope();
        return getId()
                .thenCompose(id -> {
                    String transactionsTable = getTransactionsTableName(id);

                    return CompletableFuture.allOf(Futures.exceptionallyExpecting(
                            storeHelper.deleteTable(scope, transactionsTable, false),
                            DATA_NOT_FOUND_PREDICATE, null),
                            Futures.exceptionallyExpecting(
                                    storeHelper.deleteTable(scope, getMetadataTableName(id), false),
                                    DATA_NOT_FOUND_PREDICATE, null));
                });
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            String epochTxnTable = getTransactionsTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(getScope(), metadataTable),
                    storeHelper.createTable(getScope(), epochTxnTable))
                                    .thenAccept(v -> log.debug("stream {}/{} metadata tables {} & {} created", getScope(), getName(), metadataTable,
                                            epochTxnTable));
        });
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime,
                                                                     final int startingSegmentNumber) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return Futures.exceptionallyExpecting(getCreationTime(), DATA_NOT_FOUND_PREDICATE, null)
                      .thenCompose(storedCreationTime -> {
                          if (storedCreationTime == null) {
                              return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                      configuration, creationTime, startingSegmentNumber));
                          } else {
                              return Futures.exceptionallyExpecting(getConfiguration(), DATA_NOT_FOUND_PREDICATE, null)
                                            .thenCompose(config -> {
                                                if (config != null) {
                                                    return handleConfigExists(storedCreationTime, config, startingSegmentNumber,
                                                            storedCreationTime == creationTime);
                                                } else {
                                                    return CompletableFuture.completedFuture(
                                                            new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                                                    configuration, storedCreationTime, startingSegmentNumber));
                                                }
                                            });
                          }
                      });
    }

    private CompletableFuture<CreateStreamResponse> handleConfigExists(long creationTime, StreamConfiguration config,
                                                                       int startingSegmentNumber, boolean creationTimeMatched) {
        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;
        return Futures.exceptionallyExpecting(getState(true), DATA_NOT_FOUND_PREDICATE, null)
                      .thenApply(state -> {
                          if (state == null) {
                              return new CreateStreamResponse(status, config, creationTime, startingSegmentNumber);
                          } else if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                              return new CreateStreamResponse(status, config, creationTime, startingSegmentNumber);
                          } else {
                              return new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE,
                                      config, creationTime, startingSegmentNumber);
                          }
                      });
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, CREATION_TIME_KEY, 
                        data -> BitConverter.readLong(data, 0))).thenApply(VersionedMetadata::getObject);
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, RETENTION_SET_KEY, data.toBytes())
                                      .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, RETENTION_SET_KEY));
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, RETENTION_SET_KEY, RetentionSet::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(getScope(), metadataTable, RETENTION_SET_KEY, retention.getObject().toBytes(), retention.getVersion())
                                      .thenApply(v -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, RETENTION_SET_KEY);
                                          return v;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, StreamCutRecord record) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, record.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, key, StreamCutRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(getScope(), metadataTable, key)
                                                         .thenAccept(x -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data.toBytes())
                                                         .thenAccept(x -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, key);
                    }
                    return storeHelper.getCachedData(getScope(), metadataTable, key, HistoryTimeSeries::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, VersionedMetadata<HistoryTimeSeries> data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(getScope(), metadataTable, key, data.getObject().toBytes(), data.getVersion())
                                      .thenApply(version -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, key);
                                          return version;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());

        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, CURRENT_EPOCH_KEY, epochData)
                                      .thenAccept(v -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                                      });
                });
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getObject().getEpoch());

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, CURRENT_EPOCH_KEY, epochData, data.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                    }
                    return storeHelper.getCachedData(getScope(), metadataTable, CURRENT_EPOCH_KEY, x -> BitConverter.readInt(x, 0))
                                      .thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject())
                                              .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedData(getScope(), metadataTable, key, EpochRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, SealedSegmentsMapShard data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, key, SealedSegmentsMapShard::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, key, data.getObject().toBytes(), data.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, key);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, epochData)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, key, x -> BitConverter.readInt(x, 0)));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, EPOCH_TRANSITION_KEY, epochTransition.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, EPOCH_TRANSITION_KEY)));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, EPOCH_TRANSITION_KEY, 
                        epochTransition.getObject().toBytes(), epochTransition.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, EPOCH_TRANSITION_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, EPOCH_TRANSITION_KEY, EpochTransitionRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, CREATION_TIME_KEY, b)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, CREATION_TIME_KEY)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, CONFIGURATION_KEY, configuration.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final StateRecord state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, STATE_KEY, state.toBytes())));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, key, b)));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] marker = new byte[Long.BYTES];
        BitConverter.writeLong(marker, 0, data.getObject());
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, key, marker, data.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable().thenCompose(metadataTable ->
                Futures.exceptionallyExpecting(storeHelper.getEntry(getScope(), metadataTable, key, x -> BitConverter.readLong(x, 0)),
                        DATA_NOT_FOUND_PREDICATE, null));
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(id -> storeHelper.removeEntry(getScope(), id, key));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getCurrentTxns() {
        return getTxnWithFilter((x, y) -> true, Integer.MAX_VALUE);
    }

    @Override
    public CompletableFuture<List<UUID>> getCommittingTxnsInEpoch(int epoch, int limit) {
        return getTxnWithFilter((x, y) -> RecordHelper.getTransactionEpoch(x) == epoch && 
            y.getTxnStatus().equals(TxnStatus.COMMITTING), limit)
                .thenApply(map -> {
                    List<UUID> list = map.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
                    log.info("shivesh:: found txn committing list: {}", list);
                    return list;
                })
                .whenComplete((r, e) -> {
                   if (e != null) {
                       log.info("shivesh:: fmt:: map to list conversion failed {}", e);
                   }
                });
    }

    private CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnWithFilter(
            BiFunction<UUID, ActiveTxnRecord, Boolean> filter, int limit) {
        String scope = getScope();
        Map<UUID, ActiveTxnRecord> result = new ConcurrentHashMap<>();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        AtomicReference<ByteBuf> token = new AtomicReference<>(IteratorState.EMPTY.toBytes());
        AtomicInteger shivesh = new AtomicInteger(0);
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> Futures.exceptionallyExpecting(
                        Futures.loop(canContinue::get,
                                () -> storeHelper.getEntriesPaginated(scope, epochTxnTable, token.get(), limit, ActiveTxnRecord::fromBytes)
                                                 .thenAccept(v -> {
                                                     log.info("shivesh:: found #txn {} in epoch page: {}", v.getValue().size(), shivesh.incrementAndGet());
                                                     // we exit if we have either received `limit` number of entries
                                                     List<Pair<String, VersionedMetadata<ActiveTxnRecord>>> pair = v.getValue();
                                                     for (Pair<String, VersionedMetadata<ActiveTxnRecord>> val : pair) {
                                                         UUID txnId = UUID.fromString(val.getKey());
                                                         ActiveTxnRecord txnRecord = val.getValue().getObject();
                                                         if (filter.apply(txnId, txnRecord)) {
                                                             result.put(txnId, txnRecord);
                                                             if (result.size() == limit) {
                                                                 log.info("shivesh:: met our limit requirements of {}", limit);

                                                                 break;
                                                             }
                                                         }
                                                     }
                                                     // if we get less than the requested number, then there isnt really more available. 
                                                     // it may become available but its a good exit situation for us. 
                                                     canContinue.set(!(v.getValue().size() < limit || result.size() >= limit));
                                                     if (!canContinue.get()) {
                                                         log.info("shivesh:: read all available keys.. nothing more to do.. total result = ", result.size());
                                                     } else {
                                                         log.info("shivesh: calling paginated again as we have not found our mojo.. only found {} while previous page had {}", result.size(), v.getValue().size());
                                                     }
                                                     token.set(v.getKey());
                                                 }).whenComplete((r, e) -> {
                                                     if (e!=null) {
                                                         log.info("shivesh:: error in reading all entries pagianted", e);
                                                     } else {
                                                         log.info("shivesh: exited the loop.. read all entries paginated and found {} entries.", result.size());
                                                     }
                                        }), executor)
                                   .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap()));
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord txnRecord) {
        String scope = getScope();
        return getTransactionsTable()
                .thenCompose(transactionsTable -> storeHelper.addNewEntry(scope, transactionsTable, txId.toString(), txnRecord.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId) {
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> storeHelper.getEntry(getScope(), epochTxnTable, txId.toString(), ActiveTxnRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final VersionedMetadata<ActiveTxnRecord> data) {
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> storeHelper.updateEntry(getScope(), epochTxnTable, txId.toString(), 
                        data.getObject().toBytes(), data.getVersion()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        return getTransactionsTable()
                .thenCompose(epochTransactionsTableName ->
                        storeHelper.removeEntry(getScope(), epochTransactionsTableName, txId.toString()));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final CompletedTxnRecord complete) {
        Integer batch = currentBatchSupplier.get();
        String tableName = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

        return Futures.exceptionallyComposeExpecting(
                Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, tableName, key, complete.toBytes())),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch, key, complete.toBytes()));
    }

    private CompletableFuture<Void> tryCreateBatchTable(int batch, String key, byte[] complete) {
        String batchTable = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .thenAccept(v -> log.debug("batches root table {}/{} created", NameUtils.INTERNAL_SCOPE_NAME,
                                  COMPLETED_TRANSACTIONS_BATCHES_TABLE))
                          .thenCompose(v -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                  "" + batch, new byte[0]))
                          .thenCompose(v -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, batchTable))
                          .thenCompose(v -> {
                              log.debug("batch table {} created", batchTable);
                              return Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, batchTable, key, complete));
                          });
    }

    @Override
    CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId) {
        List<Long> batches = new ArrayList<>();
        return storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .collectRemaining(x -> {
                              batches.add(Long.parseLong(x));
                              return true;
                          })
                          .thenCompose(v -> {
                              return Futures.allOfWithResults(batches.stream().map(batch -> {
                                  String table = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);
                                  String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

                                  return Futures.exceptionallyExpecting(
                                          storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, table, key, CompletedTxnRecord::fromBytes), 
                                          DATA_NOT_FOUND_PREDICATE, null);
                              }).collect(Collectors.toList()));
                          })
                          .thenCompose(result -> {
                              Optional<VersionedMetadata<CompletedTxnRecord>> any = result.stream().filter(Objects::nonNull).findFirst();
                              if (any.isPresent()) {
                                  return CompletableFuture.completedFuture(any.get());
                              } else {
                                  throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Completed Txn not found");
                              }
                          });
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncationRecord) {
        return getMetadataTable()
                .thenCompose(id -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        id, TRUNCATION_KEY, truncationRecord.toBytes())));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, TRUNCATION_KEY, 
                        truncationRecord.getObject().toBytes(), truncationRecord.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, TRUNCATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, TRUNCATION_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, TRUNCATION_KEY, StreamTruncationRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, CONFIGURATION_KEY, 
                        configuration.getObject().toBytes(), configuration.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, CONFIGURATION_KEY, StreamConfigurationRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, STATE_KEY,
                        state.getObject().toBytes(), state.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, STATE_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, STATE_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, STATE_KEY, StateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns.toBytes())));
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, 
                        CommittingTransactionsRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> update) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, 
                        update.getObject().toBytes(), update.getVersion()));
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String waitingRequestProcessor) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, WAITING_REQUEST_PROCESSOR_PATH, waitingRequestProcessor.getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    CompletableFuture<String> getWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, WAITING_REQUEST_PROCESSOR_PATH,
                        x -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(x)).toString()))
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(getScope(), metadataTable, WAITING_REQUEST_PROCESSOR_PATH));
    }

    @Override
    public void refresh() {
        String id = idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            idRef.set(null);
            // refresh all mutable records
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), STATE_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), CONFIGURATION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), TRUNCATION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), EPOCH_TRANSITION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), COMMITTING_TRANSACTIONS_RECORD_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), CURRENT_EPOCH_KEY);
        }
    }
    // endregion
}
