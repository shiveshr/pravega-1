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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.util.Config;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemoryStream extends AbstractStream {

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private Data<Integer> configuration;
    @GuardedBy("lock")
    private Data<Integer> truncationRecord;
    @GuardedBy("lock")
    private Data<Integer> state;
    @GuardedBy("lock")
    private Data<Integer> currentEpochRecord;
    @GuardedBy("lock")
    private Map<Integer, Data<Integer>> epochRecords = new HashMap<>();
    @GuardedBy("lock")
    private Data<Integer> historyIndexRoot;
    @GuardedBy("lock")
    private Map<Integer, Data<Integer>> historyIndexLeaves = new HashMap<>();
    @GuardedBy("lock")
    private Map<Integer, Data<Integer>> historyTimeSeries = new HashMap<>();
    @GuardedBy("lock")
    private Data<Integer> retentionSet;;
    @GuardedBy("lock")
    private final Map<Long, Data<Integer>> streamCutRecords = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Integer, Data<Integer>> sealedSegmentsShards = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, Data<Integer>> segmentSealingEpochs = new HashMap<>();
    @GuardedBy("lock")
    private Data<Integer> epochTransition;
    @GuardedBy("lock")
    private Data<Integer> committingTxnRecord;
    @GuardedBy("lock")
    private Data<Integer> waitingRequestNode;

    private final Object txnsLock = new Object();
    @GuardedBy("txnsLock")
    private final Map<String, Data<Integer>> activeTxns = new HashMap<>();
    @GuardedBy("txnsLock")
    private final Cache<String, Data<Integer>> completedTxns;
    private final Object markersLock = new Object();
    @GuardedBy("markersLock")
    private final Map<Long, Data<Integer>> markers = new HashMap<>();
    /**
     * This is used to guard updates to values in epoch txn map.
     * This ensures that we remove an epoch node if an only if there are no transactions against that epoch.
     * Note: there can be only two epochs at max concurrently. So using one lock for both of their updates is okay.
     */
    @GuardedBy("txnsLock")
    private final Map<Integer, Set<String>> epochTxnMap = new HashMap<>();

    InMemoryStream(String scope, String name) {
        this(scope, name, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS).toMillis());
    }

    @VisibleForTesting
    InMemoryStream(String scope, String name, long completedTxnTTL) {
        super(scope, name);
        completedTxns = CacheBuilder.newBuilder()
                .expireAfterWrite(completedTxnTTL, TimeUnit.MILLISECONDS).build();
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        synchronized (txnsLock) {
            return CompletableFuture.completedFuture(activeTxns.size());
        }
    }

    @Override
    CompletableFuture<Void> deleteStream() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<CreateStreamResponse> checkStreamExists(StreamConfiguration configuration, long timestamp, final int startingSegmentNumber) {
        CompletableFuture<CreateStreamResponse> result = new CompletableFuture<>();

        final long time;
        final StreamConfigurationRecord config;
        final Data<Integer> currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration == null ? null : StreamConfigurationRecord.parse(this.configuration.getData());
            currentState = this.state;
        }

        if (time != Long.MIN_VALUE) {
            if (config != null) {
                handleStreamMetadataExists(timestamp, result, time, startingSegmentNumber, config.getStreamConfiguration(), currentState);
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, time, startingSegmentNumber));
            }
        } else {
            result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, timestamp, startingSegmentNumber));
        }

        return result;
    }

    private void handleStreamMetadataExists(final long timestamp, CompletableFuture<CreateStreamResponse> result, final long time,
                                            final int startingSegmentNumber, final StreamConfiguration config, Data<Integer> currentState) {
        if (currentState != null) {
            State stateVal = StateRecord.parse(currentState.getData()).getState();
            if (stateVal.equals(State.UNKNOWN) || stateVal.equals(State.CREATING)) {
                CreateStreamResponse.CreateStatus status;
                status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                        CreateStreamResponse.CreateStatus.EXISTS_CREATING;
                result.complete(new CreateStreamResponse(status, config, time, startingSegmentNumber));
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, config, time, startingSegmentNumber));
            }
        } else {
            CreateStreamResponse.CreateStatus status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                    CreateStreamResponse.CreateStatus.EXISTS_CREATING;

            result.complete(new CreateStreamResponse(status, config, time, startingSegmentNumber));
        }
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long timestamp) {
        creationTime.compareAndSet(Long.MIN_VALUE, timestamp);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(byte[] config) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = new Data<>(config, 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createTruncationDataIfAbsent(byte[] truncation) {
        Preconditions.checkNotNull(truncation);

        synchronized (lock) {
            if (truncationRecord == null) {
                truncationRecord = new Data<>(truncation, 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Integer> setConfigurationData(Data<Integer> newConfig) {
        Preconditions.checkNotNull(newConfig);

        CompletableFuture<Integer> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                if (Objects.equals(this.configuration.getVersion(), newConfig.getVersion())) {
                    this.configuration = updatedCopy(new Data<>(newConfig.getData(), this.configuration.getVersion()));
                    result.complete(this.configuration.getVersion());
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getConfigurationData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(this.configuration));
        }
    }

    @Override
    CompletableFuture<Integer> setTruncationData(Data<Integer> truncationRecord) {
        Preconditions.checkNotNull(truncationRecord);

        CompletableFuture<Integer> result = new CompletableFuture<>();

        Data<Integer> copy = updatedCopy(truncationRecord);
        synchronized (lock) {
            if (this.truncationRecord == null) {
                this.truncationRecord = new Data<>(truncationRecord.getData(), 0);
            } else {
                if (Objects.equals(this.truncationRecord.getVersion(), truncationRecord.getVersion())) {
                    this.truncationRecord = copy;
                    result.complete(this.truncationRecord.getVersion());
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getTruncationData(boolean ignoreCached) {
        synchronized (lock) {
            return CompletableFuture.completedFuture(copy(this.truncationRecord));
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(byte[] state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new Data<>(state, 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Integer> setStateData(Data<Integer> newState) {
        Preconditions.checkNotNull(newState);

        CompletableFuture<Integer> result = new CompletableFuture<>();
        synchronized (lock) {
            if (Objects.equals(this.state.getVersion(), newState.getVersion())) {
                this.state = updatedCopy(newState);
                result.complete(this.state.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.state == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(state));
        }
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long key, byte[] tData) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            streamCutRecords.putIfAbsent(key, new Data<>(tData, 0));
        }
        return CompletableFuture.completedFuture(null);

    }

    @Override
    CompletableFuture<Data<Integer>> getStreamCutRecordData(long recordingTime) {
        synchronized (lock) {
            if (!this.streamCutRecords.containsKey(recordingTime)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(streamCutRecords.get(recordingTime)));
        }
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        synchronized (lock) {
            this.streamCutRecords.remove(recordingTime);

            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    CompletableFuture<Void> createHistoryTimeIndexRootIfAbsent(byte[] data) {
        Preconditions.checkNotNull(data);

        Data<Integer> copy = new Data<>(Arrays.copyOf(data, data.length), 0);
        synchronized (lock) {
            if (historyIndexRoot == null) {
                this.historyIndexRoot = copy;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTimeIndexRootNodeData() {
        synchronized (lock) {
            if (this.historyIndexRoot == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(historyIndexRoot));
        }
    }

    @Override
    CompletableFuture<Integer> updateHistoryIndexRootData(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (historyIndexRoot == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "History index root for stream: " + getName()));
            } else if (historyIndexRoot.getVersion().equals(updated.getVersion())) {
                this.historyIndexRoot = copy;
                result.complete(this.historyIndexRoot.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History index root for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTimeIndexLeafDataIfAbsent(int leafChunkNumber, byte[] data) {
        Preconditions.checkNotNull(data);

        Data<Integer> copy = new Data<>(Arrays.copyOf(data, data.length), 0);
        synchronized (lock) {
            historyIndexLeaves.putIfAbsent(leafChunkNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Integer> updateHistoryTimeIndexLeafData(int historyLeaf, Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (!historyIndexLeaves.containsKey(historyLeaf)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "History index leaf for stream: " + getName()));
            } else if (historyIndexLeaves.get(historyLeaf).getVersion().equals(updated.getVersion())) {
                this.historyIndexLeaves.put(historyLeaf, copy);
                result.complete(updated.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History index leaf for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryIndexLeafData(int leaf, boolean ignoreCached) {
        synchronized (lock) {
            if (!this.historyIndexLeaves.containsKey(leaf)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(historyIndexLeaves.get(leaf)));
        }
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        Preconditions.checkNotNull(data);

        Data<Integer> copy = new Data<>(Arrays.copyOf(data, data.length), 0);
        synchronized (lock) {
            historyTimeSeries.putIfAbsent(chunkNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        synchronized (lock) {
            if (!this.historyTimeSeries.containsKey(chunkNumber)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(historyTimeSeries.get(chunkNumber)));
        }
    }

    @Override
    CompletableFuture<Integer> updateHistoryTimeSeriesChunkData(int historyChunk, Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (!historyTimeSeries.containsKey(historyChunk)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "History timeseries chunk for stream: " + getName()));
            } else if (historyTimeSeries.get(historyChunk).getVersion().equals(updated.getVersion())) {
                this.historyTimeSeries.put(historyChunk, copy);
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History time series for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                this.committingTxnRecord = new Data<>(data, 0);
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> updateCurrentEpochRecordData(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (currentEpochRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "current epoch record for stream: " + getName()));
            } else if (currentEpochRecord.getVersion().equals(updated.getVersion())) {
                this.currentEpochRecord = copy;
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "current epoch record for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCurrentEpochRecordData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.currentEpochRecord));
        }
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.epochRecords.putIfAbsent(epoch, new Data<>(data, 0));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getEpochRecordData(int epoch) {
        synchronized (lock) {
            if (!this.epochRecords.containsKey(epoch)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.epochRecords.get(epoch)));
        }
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, byte[] data) {
        Preconditions.checkNotNull(data);

        Data<Integer> copy = new Data<>(Arrays.copyOf(data, data.length), 0);
        synchronized (lock) {
            sealedSegmentsShards.putIfAbsent(shardNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getSealedSegmentSizesMapShardData(int shard) {
        synchronized (lock) {
            if (!this.sealedSegmentsShards.containsKey(shard)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(sealedSegmentsShards.get(shard)));
        }
    }

    @Override
    CompletableFuture<Integer> updateSealedSegmentSizesMapShardData(int shard, Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (!sealedSegmentsShards.containsKey(shard)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "sealed segment size map shard for stream: " + getName()));
            } else if (sealedSegmentsShards.get(shard).getVersion().equals(updated.getVersion())) {
                this.sealedSegmentsShards.put(shard, copy);
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History time series for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segment, int epoch) {
        Preconditions.checkNotNull(epoch);
        byte[] array = new byte[Integer.BYTES];
        BitConverter.writeInt(array, 0, epoch);

        synchronized (lock) {
            segmentSealingEpochs.putIfAbsent(segment, new Data<>(array, 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentSealedRecordData(long segmentId) {
        synchronized (lock) {
            if (!this.segmentSealingEpochs.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(segmentSealingEpochs.get(segmentId)));
        }
    }

    @Override
    CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime) {
        Preconditions.checkNotNull(txId);

        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Data<Integer> txnData = new Data<>(
                new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime, TxnStatus.OPEN)
                        .toByteArray(), 0);
        int epoch = getTransactionEpoch(txId);

        synchronized (txnsLock) {
            activeTxns.putIfAbsent(txId.toString(), txnData);
            epochTxnMap.compute(epoch, (x, y) -> {
                if (y == null) {
                    y = new HashSet<>();
                }
                y.add(txId.toString());
                return y;
            });
            result.complete(null);
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(int epoch, UUID txId) {
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }

            return CompletableFuture.completedFuture(copy(activeTxns.get(txId.toString())));
        }
    }

    @Override
    CompletableFuture<Integer> updateActiveTx(int epoch, UUID txId, Data<Integer> data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Integer> result = new CompletableFuture<>();
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            } else {
                activeTxns.compute(txId.toString(), (x, y) -> new Data<>(data.getData(), y.getVersion() + 1));
                result.complete(activeTxns.get(txId.toString()).getVersion());
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(UUID txId) {
        Preconditions.checkNotNull(txId);
        synchronized (txnsLock) {
            Data<Integer> value = completedTxns.getIfPresent(txId.toString());
            if (value == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }
            return CompletableFuture.completedFuture(copy(value));
        }
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(int epoch, UUID txId) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            activeTxns.remove(txId.toString());
            epochTxnMap.computeIfPresent(epoch, (x, y) -> {
                y.remove(txId.toString());
                return y;
            });

            if (epochTxnMap.get(epoch).isEmpty()) {
                epochTxnMap.remove(epoch);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, byte[] complete) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            Data<Integer> value = completedTxns.getIfPresent(txId.toString());
            if (value == null) {
                completedTxns.put(txId.toString(), new Data<>(complete, 0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);
        synchronized (markersLock) {
            markers.putIfAbsent(segmentId, new Data<>(b, 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Integer> updateMarkerData(long segmentId, Data<Integer> data) {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> next = updatedCopy(data);
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment number: " + segmentId));
            } else {
                markers.compute(segmentId, (x, y) -> {
                    if (y.getVersion().equals(data.getVersion())) {
                        result.complete(next.getVersion());
                        return next;
                    } else {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                "Stream: " + getName() + " Segment number: " + segmentId));
                        return y;
                    }
                });
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        synchronized (markersLock) {
            markers.remove(segmentId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(long segmentId) {
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment: " + segmentId));
            }
            return CompletableFuture.completedFuture(copy(markers.get(segmentId)));
        }
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        synchronized (txnsLock) {
            Map<String, Data<Integer>> map = activeTxns.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(map));
        }
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getTxnInEpoch(int epoch) {
        synchronized (txnsLock) {
            Set<String> transactions = epochTxnMap.get(epoch);
            Map<String, Data<Integer>> map;
            if (transactions != null) {
                map = activeTxns.entrySet().stream().filter(x -> transactions.contains(x.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
                map = Collections.unmodifiableMap(map);
            } else {
                map = Collections.emptyMap();
            }
            return CompletableFuture.completedFuture(map);
        }
    }

    @Override
    CompletableFuture<Void> checkScopeExists() throws StoreException {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] retention) {
        Preconditions.checkNotNull(retention);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.retentionSet = new Data<>(retention, 0);
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getRetentionSetData() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                result.complete(copy(retentionSet));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> updateRetentionSetData(Data<Integer> retention) {
        Preconditions.checkNotNull(retention);
        Preconditions.checkNotNull(retention.getData());

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        Data<Integer> next = updatedCopy(retention);
        synchronized (lock) {
            if (retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "retentionSet for stream: " + getName()));
            } else if (retentionSet.getVersion().equals(retention.getVersion())) {
                this.retentionSet = next;
                result.complete(next.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "retentionSet for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransitionData) {
        Preconditions.checkNotNull(epochTransitionData);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition != null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_EXISTS, "epoch transition exists"));
            } else {
                this.epochTransition = new Data<>(epochTransitionData, 0);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> updateEpochTransitionNode(Data<Integer> update) {
        Preconditions.checkNotNull(update);

        CompletableFuture<Integer> result = new CompletableFuture<>();
        byte[] copy = Arrays.copyOf(update.getData(), update.getData().length);
        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else {
                this.epochTransition = new Data<>(copy, this.epochTransition.getVersion() + 1);
                result.complete(this.epochTransition.getVersion());
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getEpochTransitionNode() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else {
                result.complete(copy(epochTransition));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(byte[] committingTxns) {
        Preconditions.checkNotNull(committingTxns);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord != null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_EXISTS, "committing transactions record exists"));
            } else {
                this.committingTxnRecord = new Data<>(Arrays.copyOf(committingTxns, committingTxns.length), 0);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCommitTxnRecord() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "committing transactions not found"));
            } else {
                result.complete(copy(committingTxnRecord));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> updateCommitTxnRecord(Data<Integer> update) {
        Preconditions.checkNotNull(committingTxnRecord);

        CompletableFuture<Integer> result = new CompletableFuture<>();
        byte[] copy = Arrays.copyOf(update.getData(), update.getData().length);
        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "committing txn record not found"));
            } else {
                this.committingTxnRecord = new Data<>(copy, this.committingTxnRecord.getVersion() + 1);
                result.complete(this.committingTxnRecord.getVersion());
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data) {
        synchronized (lock) {
            if (waitingRequestNode == null) {
                waitingRequestNode = new Data<>(data, 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getWaitingRequestNode() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.waitingRequestNode == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "waiting request node not found"));
            } else {
                result.complete(copy(waitingRequestNode));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        synchronized (lock) {
            this.waitingRequestNode = null;
        }
        return CompletableFuture.completedFuture(null);
    }

    private Data<Integer> copy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion());
    }

    private Data<Integer> updatedCopy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion() + 1);
    }
}
