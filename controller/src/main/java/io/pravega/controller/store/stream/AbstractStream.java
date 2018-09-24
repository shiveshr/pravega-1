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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.store.stream.StoreException.DataNotFoundException;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryTimeIndexLeaf;
import io.pravega.controller.store.stream.tables.HistoryTimeIndexRootNode;
import io.pravega.controller.store.stream.tables.EpochRecord;
import io.pravega.controller.store.stream.tables.HistoryTimeSeries;
import io.pravega.controller.store.stream.tables.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.tables.RetentionSet;
import io.pravega.controller.store.stream.tables.RetentionSetRecord;
import io.pravega.controller.store.stream.tables.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Lombok;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;
import static java.util.stream.Collectors.toMap;

@Slf4j
public abstract class AbstractStream<T> implements Stream {

    // TODO: shivesh move it to constant
    public static final int SHARD_SIZE = 10000;
    public static final int HISTORY_INDEX_CHUNK_SIZE = 100000;
    public static final int HISTORY_CHUNK_SIZE = 10000;
    private final String scope;
    private final String name;

    AbstractStream(final String scope, final String name) {
        this.scope = scope;
        this.name = name;
    }

    @Override
    public String getScope() {
        return this.scope;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getScopeName() {
        return this.scope;
    }

    // region create delete

    /***
     * Creates a new stream record in the stream store.
     * Create a new task of type Create.
     * If create task already exists, use that and bring it to completion
     * If no task exists, fall through all create steps. They are all idempotent
     * <p>
     * Create Steps:
     * 1. Create new store configuration
     * 2. Create new segment table.
     * 3. Create new history table.
     * 4. Create new index
     *
     * @param configuration stream configuration.
     * @return : future of whether it was done or not
     */
    @Override
    public CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, long createTimestamp, int startingSegmentNumber) {
        return checkScopeExists()
                .thenCompose((Void v) -> checkStreamExists(configuration, createTimestamp, startingSegmentNumber))
                .thenCompose(createStreamResponse -> storeCreationTimeIfAbsent(createStreamResponse.getTimestamp())
                        .thenCompose((Void v)-> createConfigurationIfAbsent(StreamConfigurationRecord.complete(
                            createStreamResponse.getConfiguration())))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamTruncationRecord.EMPTY))
                        .thenCompose((Void v) -> createStateIfAbsent(State.CREATING))
                        .thenCompose((Void v) -> {
                            final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
                            // create epoch 0 record
                            final double keyRangeChunk = 1.0 / numSegments;

                            long creationTime = createStreamResponse.getTimestamp();
                            final List<SegmentRecord> segments = IntStream.range(0, numSegments)
                                    .boxed()
                                    .map(x -> newSegmentRecord(0, startingSegmentNumber + x, creationTime,
                                            x * keyRangeChunk, (x + 1) * keyRangeChunk))
                                    .collect(Collectors.toList());

                            EpochRecord epoch0 = EpochRecord.builder().epoch(0).referenceEpoch(0).segments(segments)
                                    .creationTime(creationTime).build();

                            return createEpochRecord(epoch0)
                                    .thenCompose(r -> createHistoryIndex(creationTime))
                                    .thenCompose(r -> {
                                        HistoryTimeSeriesRecord record = HistoryTimeSeriesRecord.builder().epoch(0).referenceEpoch(0)
                                                .segmentsCreated(epoch0.getSegments()).segmentsSealed(Collections.emptyList())
                                                .creationTime(epoch0.getCreationTime()).build();
                                        return createHistoryTimeSeriesChunk(0, record);
                                    })
                                    .thenCompose(r -> createSealedSegmentSizeMapShardIfAbsent(0))
                                    .thenCompose(r -> createRetentionSetDataIfAbsent(new RetentionSet(Collections.emptyList()).toByteArray()))
                                    .thenCompose(r -> createCurrentEpochRecordDataIfAbsent(epoch0.toByteArray()));
                        })
                        .thenApply((Void v) -> createStreamResponse));
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
    }
    // endregion

    // region truncation
    @Override
    public CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut) {
        return computeEpochCutMap(streamCut)
                .thenCompose(epochCutMap -> getTruncationData(true)
                        .thenCompose(truncationData -> {
                            Preconditions.checkNotNull(truncationData);
                            StreamTruncationRecord previous = StreamTruncationRecord.parse(truncationData.getData());
                            Exceptions.checkArgument(!previous.isUpdating(), "TruncationRecord", "Truncation record conflict");

                            // check greater than
                            Exceptions.checkArgument(epochCutMap.keySet().stream().allMatch(x ->
                                            previous.getCutEpochMap().keySet().stream().noneMatch(y -> y.overlaps(x) && y.segmentId() > x.segmentId())),
                                    "StreamCut", "Greater than previous truncation point");

                            return computeTruncationRecord(previous, streamCut, epochCutMap)
                                    .thenCompose(prop -> setTruncationData(new Data<>(prop.toByteArray(), truncationData.getVersion())));
                        }));
    }

    private CompletableFuture<StreamTruncationRecord> computeTruncationRecord(StreamTruncationRecord previous,
                                                                              Map<Long, Long> streamCut,
                                                                              Map<SegmentRecord, Integer> epochCutMap) {
        log.debug("computing truncation for stream {}/{}", scope, name);
        // compute segments to delete between previous and streamcut.

        // find segments between "previous" stream cut and current stream cut. these are segments to delete.
        // Note: exclude segments in current streamcut
        CompletableFuture<Map<SegmentRecord, Integer>> mapFromFuture = previous.getCutEpochMap().isEmpty() ?
                getEpochRecord(0).thenApply(epoch -> epoch.getSegments().stream().collect(Collectors.toMap(x -> x, x -> epoch.getEpoch())))
                : CompletableFuture.completedFuture(previous.getCutEpochMap());

        return mapFromFuture
                .thenCompose(mapFrom -> segmentsBetweenStreamCuts(mapFrom, epochCutMap)
                .thenApply(segmentsBetween -> {
                    // toDelete =
                    // all segments in between
                    // all segments from previous that are not present in streamCut
                    List<Long> toDelete = segmentsBetween.stream().filter(x -> !streamCut.containsKey(x))
                            .map(SegmentRecord::segmentId).collect(Collectors.toList());

                    return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                            previous.getDeletedSegments(), ImmutableSet.copyOf(toDelete), true);
                }));
    }

    @Override
    public CompletableFuture<Void> completeTruncation() {
        return checkState(state -> state.equals(State.TRUNCATING))
                .thenCompose(v -> getTruncationData(true)
                        .thenCompose(truncationData -> {
                            Preconditions.checkNotNull(truncationData);
                            StreamTruncationRecord current = StreamTruncationRecord.parse(truncationData.getData());
                            if (current.isUpdating()) {
                                StreamTruncationRecord completedProp = StreamTruncationRecord.complete(current);

                                return setTruncationData(new Data<>(completedProp.toByteArray(), truncationData.getVersion()));
                            } else {
                                // idempotent
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
    }

    @Override
    public CompletableFuture<StreamTruncationRecord> getTruncationRecord(boolean ignoreCached) {
        return getTruncationData(ignoreCached)
                .thenApply(data -> data == null ? StreamTruncationRecord.EMPTY : StreamTruncationRecord.parse(data.getData()));
    }
    // endregion

    // region configuration
    /**
     * Update configuration at configurationPath.
     *
     * @param newConfiguration new stream configuration.
     * @return future of operation.
     */
    @Override
    public CompletableFuture<Void> startUpdateConfiguration(final StreamConfiguration newConfiguration) {
        return getConfigurationData(true)
                .thenCompose(configData -> {
                    StreamConfigurationRecord previous = StreamConfigurationRecord.parse(configData.getData());
                    Preconditions.checkNotNull(previous);
                    Preconditions.checkArgument(!previous.isUpdating());
                    StreamConfigurationRecord update = StreamConfigurationRecord.update(newConfiguration);
                    return setConfigurationData(new Data<>(update.toByteArray(), configData.getVersion()));
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @return future of operation
     */
    @Override
    public CompletableFuture<Void> completeUpdateConfiguration() {
        return checkState(state -> state.equals(State.UPDATING))
                .thenCompose(v -> getConfigurationData(true)
                        .thenCompose(configData -> {
                            StreamConfigurationRecord current = StreamConfigurationRecord.parse(configData.getData());
                            Preconditions.checkNotNull(current);
                            if (current.isUpdating()) {
                                StreamConfigurationRecord newProperty = StreamConfigurationRecord.complete(current.getStreamConfiguration());
                                log.debug("Completing update configuration for stream {}/{}", scope, name);
                                return setConfigurationData(new Data<>(newProperty.toByteArray(), configData.getVersion()));
                            } else {
                                // idempotent
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationRecord(false).thenApply(StreamConfigurationRecord::getStreamConfiguration);
    }

    @Override
    public CompletableFuture<StreamConfigurationRecord> getConfigurationRecord(boolean ignoreCached) {
        return getConfigurationData(ignoreCached)
                .thenApply(data -> StreamConfigurationRecord.parse(data.getData()));
    }
    // endregion

    // region state
    @Override
    public CompletableFuture<Boolean> updateState(final State state) {
        return getStateData(true)
                .thenCompose(currState -> {
                    if (State.isTransitionAllowed(StateRecord.parse(currState.getData()).getState(), state)) {
                        return setStateData(new Data<>(StateRecord.builder().state(state).build().toByteArray(), currState.getVersion()))
                                .thenApply(x -> true);
                    } else {
                        return Futures.failedFuture(StoreException.create(
                                StoreException.Type.OPERATION_NOT_ALLOWED,
                                "Stream: " + getName() + " State: " + state.name() + " current state = " + StateRecord.parse(currState.getData()).getState()));
                    }
                });
    }

    @Override
    public CompletableFuture<State> getState(boolean ignoreCached) {
        return getStateData(ignoreCached)
                .thenApply(x -> StateRecord.parse(x.getData()).getState());
    }

    /**
     * Reset state of stream to ACTIVE if it matches the supplied state.
     * @param state stream state to match
     * @return Future which when completes will have reset the state or failed with appropriate exception.
     */
    @Override
    public CompletableFuture<Void> resetStateConditionally(State state) {
        return Futures.toVoid(getState(true)
                .thenCompose(currState -> {
                    if (currState.equals(state)) {
                        return updateState(State.ACTIVE);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }));
    }

    private CompletableFuture<Void> checkState(Predicate<State> predicate) {
        return getState(true)
                .thenAccept(currState -> {
                    if (!predicate.test(currState)) {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " Current State: " + currState.name());
                    }
                });
    }

    private CompletableFuture<Void> verifyLegalState() {
        return getState(false).thenApply(state -> {
            if (state == null || state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    private CompletableFuture<Void> verifyNotSealed() {
        return getState(false).thenApply(state -> {
            if (state.equals(State.SEALING) || state.equals(State.SEALED)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    // endregion

    // region history
    /**
     * Fetches Segment metadata from the epoch in which segment was created.
     *
     * @param segmentId segment id.
     * @return : Future, which when complete contains segment object
     */
    @Override
    public CompletableFuture<Segment> getSegment(final long segmentId) {
        // extract epoch from segment id.
        // fetch epoch record for the said epoch
        // extract segment record from it.
        int epoch = StreamSegmentNameUtils.getEpoch(segmentId);
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> {
                    Optional<SegmentRecord> segmentRecord = epochRecord.getSegments().stream()
                            .filter(x -> x.segmentId() == segmentId).findAny();
                    if (segmentRecord.isPresent()) {
                        return transform(segmentRecord.get());
                    } else {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "segment not found in epoch");
                    }
                });
    }

    @Override
    public CompletableFuture<Map<Segment, List<Long>>> getSuccessorsWithPredecessors(final long segmentId) {
        // get segment sealed record.
        // fetch segment sealed record.
        return getSegmentSealedEpoch(segmentId)
                .thenCompose(sealedEpoch -> {
                    // if sealed record exists. fetch its sealing epoch.
                    // Note: sealed record is created even before the segment is sealed. So if client is requesting for successor,
                    // we should find it.
                    CompletableFuture<EpochRecord> sealedEpochFuture = getEpochRecord(sealedEpoch);

                    // fetch previous epoch as well.
                    CompletableFuture<EpochRecord> previousEpochFuture = getEpochRecord(sealedEpoch - 1);

                    return CompletableFuture.allOf(sealedEpochFuture, previousEpochFuture)
                            .thenApply(x -> {
                                EpochRecord sealedEpochRecord = sealedEpochFuture.join();
                                EpochRecord previousEpochRecord = previousEpochFuture.join();
                                Optional<SegmentRecord> segmentOpt = previousEpochRecord.getSegments().stream()
                                        .filter(r -> r.segmentId() == segmentId).findAny();
                                assert (segmentOpt.isPresent());
                                SegmentRecord segment = segmentOpt.get();

                                List<SegmentRecord> successors = sealedEpochRecord.getSegments().stream()
                                        .filter(r -> r.overlaps(segment)).collect(Collectors.toList());

                                return successors.stream()
                                        .collect(Collectors.toMap(this::transform, z -> previousEpochRecord.getSegments()
                                                .stream().filter(predecessor -> predecessor.overlaps(z))
                                                .map(SegmentRecord::segmentId).collect(Collectors.toList())));
                            });
                });
    }

    private Segment transform(SegmentRecord segmentRecord) {
        return new Segment(segmentRecord.segmentId(), segmentRecord.getCreationTime(),
                segmentRecord.getKeyStart(), segmentRecord.getKeyEnd());
    }

    private List<Segment> transform(List<SegmentRecord> segmentRecords) {
        return segmentRecords.stream().map(this::transform).collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments() {
        // read current epoch record
        return getCurrentEpochRecord().thenApply(epochRecord -> transform(epochRecord.getSegments()));
    }

    /**
     *
     * @param timestamp point in time.
     * @return : list of active segment numbers at given time stamp
     */
    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(final long timestamp) {
        return getTruncationRecord(true)
                .thenCompose(truncationRecord -> findEpochAtTime(timestamp)
                        .thenCompose(this::getEpochRecord)
                        .thenApply(epochRecord -> TableHelper.getActiveSegments(epochRecord, truncationRecord)))
                .thenApply(result -> result.entrySet().stream().collect(Collectors.toMap(x -> transform(x.getKey()), x -> x.getValue())));
    }

    @Override
    public CompletableFuture<List<Segment>> getSegmentsInEpoch(final int epoch) {
        // read history record for the epoch
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> transform(epochRecord.getSegments()));
    }

    @Override
    public CompletableFuture<EpochTransitionRecord> getEpochTransition() {
        return getEpochTransitionNode().thenApply(x -> EpochTransitionRecord.parse(x.getData()));
    }

    @Override
    public CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(currentEpochRecord -> EpochRecord.parse(currentEpochRecord.getData()));
    }

    @Override
    public CompletableFuture<EpochRecord> getEpochRecord(int epoch) {
        return getEpochRecordData(epoch).thenApply(epochRecordData -> EpochRecord.parse(epochRecordData.getData()));
    }

    private CompletableFuture<Void> createHistoryIndex(long time) {
        return createHistoryTimeIndexRootIfAbsent(new HistoryTimeIndexRootNode(Lists.newArrayList(time)).toByteArray())
                .thenCompose(v -> {
                    HistoryTimeIndexLeaf historyIndexLeaf = new HistoryTimeIndexLeaf(Lists.newArrayList(time));
                    return createHistoryTimeIndexLeafDataIfAbsent(0, historyIndexLeaf.toByteArray());
                });
    }

    private CompletableFuture<Void> updateHistoryIndex(int epoch, long time) {
        // compute history leaf
        int historyLeaf = epoch / HISTORY_INDEX_CHUNK_SIZE;
        boolean isFirst = epoch % HISTORY_INDEX_CHUNK_SIZE == 0;
        if (isFirst) {
            return getHistoryTimeIndexRootNodeData()
                    .thenCompose(indexRootData -> {
                        HistoryTimeIndexRootNode indexRoot = HistoryTimeIndexRootNode.parse(indexRootData.getData());
                        HistoryTimeIndexRootNode update = HistoryTimeIndexRootNode.addNewLeaf(indexRoot, time);
                        return updateHistoryIndexRootData(new Data<>(update.toByteArray(), indexRootData.getVersion()));
                    })
                    .thenCompose(v -> {
                        HistoryTimeIndexLeaf historyIndexLeaf = new HistoryTimeIndexLeaf(Lists.newArrayList(time));
                        return createHistoryTimeIndexLeafDataIfAbsent(historyLeaf, historyIndexLeaf.toByteArray());
                    });
        } else {
            return getHistoryIndexLeafData(historyLeaf, true)
                    .thenCompose(historyLeafData -> {
                        HistoryTimeIndexLeaf leaf = HistoryTimeIndexLeaf.parse(historyLeafData.getData());
                        if (historyLeaf * HISTORY_INDEX_CHUNK_SIZE + leaf.getRecords().size() - 1 < epoch) {
                            HistoryTimeIndexLeaf update = HistoryTimeIndexLeaf.addRecord(leaf, time);
                            return updateHistoryTimeIndexLeafData(historyLeaf, new Data<>(update.toByteArray(), historyLeafData.getVersion()));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
    }

    private CompletableFuture<HistoryTimeIndexRootNode> getHistoryIndexRootNode() {
        return getHistoryTimeIndexRootNodeData().thenApply(x -> HistoryTimeIndexRootNode.parse(x.getData()));
    }

    private CompletableFuture<HistoryTimeIndexLeaf> getHistoryIndexLeaf(int leaf, boolean ignoreCached) {
        return getHistoryIndexLeafData(leaf, ignoreCached).thenApply(x -> HistoryTimeIndexLeaf.parse(x.getData()));
    }

    private CompletableFuture<Integer> findEpochAtTime(long timestamp) {
        // fetch history index BTree root and find the leaf node corresponding to given time.
        // fetch the leaf node and perform binary search in the leaf node to find the epoch corresponding to given time
        return getHistoryIndexRootNode()
                .thenCompose(root -> {
                    int leaf = root.findLeafNode(timestamp);
                    boolean isLatest = root.getLeaves().size() == leaf;
                    return getHistoryIndexLeaf(leaf, isLatest)
                            .thenApply(leafNode -> leafNode.findIndexAtTime(timestamp))
                            .thenApply(index -> leaf * HISTORY_INDEX_CHUNK_SIZE + index);
                });
    }

    private CompletableFuture<Void> createHistoryTimeSeriesChunk(int chunkNumber, HistoryTimeSeriesRecord epoch) {
        HistoryTimeSeries timeSeries = HistoryTimeSeries.builder().build();
        HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(timeSeries, epoch);
        return createHistoryTimeSeriesChunkDataIfAbsent(chunkNumber, update.toByteArray());
    }

    private CompletableFuture<Void> updateHistoryTimeSeries(HistoryTimeSeriesRecord record) {
        int historyChunk = record.getEpoch() / HISTORY_CHUNK_SIZE;
        boolean isFirst = record.getEpoch() % HISTORY_CHUNK_SIZE == 0;

        if (isFirst) {
            return createHistoryTimeSeriesChunk(historyChunk, record);
        } else {
            return getHistoryTimeSeriesChunkData(historyChunk, true)
                    .thenCompose(x -> {
                        HistoryTimeSeries historyChunkTimeSeries = HistoryTimeSeries.parse(x.getData());
                        if (historyChunkTimeSeries.getLatestRecord().getEpoch() < record.getEpoch()) {
                            HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(historyChunkTimeSeries, record);
                            return updateHistoryTimeSeriesChunkData(historyChunk, new Data<>(update.toByteArray(), x.getVersion()));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
    }

    private CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, boolean ignoreCached) {
        return getHistoryTimeSeriesChunkData(chunkNumber, ignoreCached)
                .thenApply(x -> HistoryTimeSeries.parse(x.getData()));
    }

    private CompletableFuture<List<EpochRecord>> fetchEpochs(int fromEpoch, int toEpoch) {
        // TODO: shivesh verify reduce function!!!
        // fetch history time series chunk corresponding to from.
        // read entries till either last entry or till to
        // if to is not in this chunk fetch the next chunk and read till to
        // keep doing this until all records till to have been read.
        // keep computing history record from history time series by applying delta on previous.

        return getCurrentEpochRecord()
                .thenApply(currentEpoch -> currentEpoch.getEpoch() / HISTORY_CHUNK_SIZE)
                .thenCompose(latestChunkNumber ->
                        Futures.allOfWithResults(IntStream.range(fromEpoch / HISTORY_CHUNK_SIZE, toEpoch / HISTORY_CHUNK_SIZE)
                        .mapToObj(i -> {
                            int firstEpoch = i * HISTORY_CHUNK_SIZE > fromEpoch ? i * HISTORY_CHUNK_SIZE : fromEpoch;
                            return getEpochRecord(firstEpoch)
                                    .thenCompose(first -> getHistoryTimeSeriesChunk(i, i >= latestChunkNumber)
                                            .thenApply(x -> {
                                                ArrayList<EpochRecord> identity = Lists.newArrayList(first);

                                                return x.getHistoryRecords().stream().filter(r -> r.getEpoch() > fromEpoch && r.getEpoch() <= toEpoch)
                                                        .reduce(identity, (r, s) -> {
                                                            EpochRecord next = getNewEpochRecord(r.get(r.size() - 1),
                                                                    s.getEpoch(), s.getReferenceEpoch(), s.getSegmentsCreated(),
                                                                    s.getSegmentsSealed().stream().map(SegmentRecord::segmentId)
                                                                            .collect(Collectors.toList()), s.getScaleTime());
                                                            return Lists.newArrayList(next);
                                                        }, (r, s) -> {
                                                            ArrayList<EpochRecord> list = new ArrayList<>(r);
                                                            list.addAll(s);
                                                            return list;
                                                        });
                                            }));
                        }).collect(Collectors.toList()))).thenApply(c -> c.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> createEpochRecord(EpochRecord epoch) {
        return createEpochRecordDataIfAbsent(epoch.getEpoch(), epoch.toByteArray());
    }

    private CompletableFuture<Void> recordSegmentSealedEpoch(long segmentToSeal, int newEpoch) {
        return createSegmentSealedEpochRecordData(segmentToSeal, newEpoch);
    }

    private CompletableFuture<Integer> getSegmentSealedEpoch(long segmentId) {
        return getSegmentSealedRecordData(segmentId).thenApply(x -> BitConverter.readInt(x.getData(), 0));
    }

    private CompletableFuture<EpochRecord> getCurrentEpochRecord() {
        return getCurrentEpochRecordData(false).thenApply(x -> EpochRecord.parse(x.getData()));
    }

    private CompletableFuture<Void> updateCurrentEpochRecord(int activeEpoch) {
        return getEpochRecord(activeEpoch)
                .thenCompose(epochRecord -> getCurrentEpochRecordData(true)
                        .thenCompose(currentEpochRecordData -> updateCurrentEpochRecordData(
                                new Data<>(epochRecord.toByteArray(), currentEpochRecordData.getVersion()))));
    }

    private CompletableFuture<Void> createSealedSegmentSizeMapShardIfAbsent(int shardNumber) {
        SealedSegmentsMapShard shard = new SealedSegmentsMapShard(shardNumber, Collections.emptyMap());
        return createSealedSegmentSizesMapShardDataIfAbsent(shardNumber, shard.toByteArray());
    }

    private CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(int shard) {
        return getSealedSegmentSizesMapShardData(shard).thenApply(x -> SealedSegmentsMapShard.parse(x.getData()));
    }

    private CompletableFuture<Void> updateSealedSegmentSizes(Map<Long, Long> sealedSegmentSizes) {
        Map<Integer, List<Long>> shards = sealedSegmentSizes.values().stream()
                .collect(Collectors.groupingBy(x -> StreamSegmentNameUtils.getSegmentNumber(x) / SHARD_SIZE));
        return Futures.allOf(shards.entrySet().stream().map(x -> {
            int shard = x.getKey();
            List<Long> segments = x.getValue();

            return createSealedSegmentSizeMapShardIfAbsent(shard).thenCompose(v -> getSealedSegmentSizesMapShardData(shard)
                    .thenApply(y -> {
                        SealedSegmentsMapShard mapShard = SealedSegmentsMapShard.parse(y.getData());
                        segments.forEach(z -> mapShard.addSealedSegmentSize(z, sealedSegmentSizes.get(z)));
                        return updateSealedSegmentSizesMapShardData(shard, new Data<>(mapShard.toByteArray(), y.getVersion()));
                    }));
        }).collect(Collectors.toList()));
    }

    private EpochRecord getNewEpochRecord(final EpochRecord lastRecord, final int epoch, final int referenceEpoch,
                                          final Collection<SegmentRecord> createdSegments, final Collection<Long> sealedSegments, final long time) {
        List<SegmentRecord> segments = lastRecord.getSegments();
        segments = segments.stream().filter(x -> sealedSegments.stream().anyMatch(y -> y == x.segmentId())).collect(Collectors.toList());
        segments.addAll(createdSegments);
        return EpochRecord.builder().epoch(epoch).referenceEpoch(referenceEpoch).segments(segments).creationTime(time).build();
    }
    //endregion

    // region scale metadata
    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final long from, final long to) {
        // fetch history index and find epochs corresponding to "from" and "to"
        // fetch "from epoch" from epoch record
        // fetch epochs from history timeseries.
        //
        return verifyLegalState()
                .thenCompose(v -> {
                    CompletableFuture<Integer> fromEpoch = findEpochAtTime(from);
                    CompletableFuture<Integer> toEpoch = findEpochAtTime(to);
                    CompletableFuture<List<EpochRecord>> records = CompletableFuture.allOf(fromEpoch, toEpoch)
                            .thenCompose(x -> {
                                // fetch epochs will fetch it from history time series. this will be efficient.
                                return fetchEpochs(fromEpoch.join(), toEpoch.join());
                            });
                    // retrieve epochs between from and to
                    // this will hammer the store with a lot of calls if number of epochs between from and to are high
                    return records;
                })
                .thenApply(this::mapToScaleMetadata);
    }

    private List<ScaleMetadata> mapToScaleMetadata(List<EpochRecord> epochRecords) {
        final AtomicReference<List<SegmentRecord>> previous = new AtomicReference<>();
        return epochRecords.stream()
                .map(record -> {
                    long splits = 0;
                    long merges = 0;
                    List<SegmentRecord> segments = record.getSegments();
                    if (previous.get() != null) {
                        splits = findSegmentSplitsMerges(previous.get(), segments);
                        merges = findSegmentSplitsMerges(segments, previous.get());
                    }
                    previous.set(segments);
                    return new ScaleMetadata(record.getCreationTime(), transform(segments), splits, merges);
                }).collect(Collectors.toList());
    }

    /**
     * Method to calculate number of splits and merges.
     *
     * Principle to calculate the number of splits and merges:
     * 1- An event has occurred if a reference range is present (overlaps) in at least two consecutive target ranges.
     * 2- If the direction of the check in 1 is forward, then it is a split, otherwise it is a merge.
     *
     * @param referenceSegmentsList Reference segment list.
     * @param targetSegmentsList Target segment list.
     * @return Number of splits/merges.
     */
    private long findSegmentSplitsMerges(List<SegmentRecord> referenceSegmentsList, List<SegmentRecord> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }
    //endregion

    //region streamcut
    @Override
    public CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to) {
        // compute epoch cut map for from till to
        // if from is empty we need to start from epoch 0.
        // if to is empty we need to go on till current epoch.
        CompletableFuture<Map<SegmentRecord, Integer>> mapFromFuture = from.isEmpty() ?
                getEpochRecord(0).thenApply(epoch -> epoch.getSegments().stream().collect(Collectors.toMap(x -> x, x -> epoch.getEpoch())))
                : computeEpochCutMap(from);
        CompletableFuture<Map<SegmentRecord, Integer>> mapToFuture = to.isEmpty() ?
                getCurrentEpochRecord().thenApply(epoch -> epoch.getSegments().stream().collect(Collectors.toMap(x -> x, x -> epoch.getEpoch())))
                : computeEpochCutMap(to);

        return CompletableFuture.allOf(mapFromFuture, mapToFuture)
                .thenCompose(x -> segmentsBetweenStreamCuts(mapFromFuture.join(), mapToFuture.join()))
                .thenApply(this::transform);
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(Map<Long, Long> streamCut) {
        return Futures.allOfWithResults(streamCut.keySet().stream().map(x -> getSegment(x).thenApply(segment ->
                new SimpleEntry<>(segment.getKeyStart(), segment.getKeyEnd())))
                .collect(Collectors.toList()))
                .thenAccept(TableHelper::validateStreamCut)
                .handle((r, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                            return false;
                        } else {
                            log.warn("Exception while trying to validate a stream cut for stream {}/{}", scope, name);
                            throw Lombok.sneakyThrow(e);
                        }
                    } else {
                        return true;
                    }
                });
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut, Optional<StreamCutRecord> reference) {
        Map<Long, Long> referenceStreamCut = reference.map(streamCutRecord -> streamCutRecord.getStreamCut().entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().segmentId(), Map.Entry::getValue))).orElse(Collections.emptyMap());
        return getSegmentsBetweenStreamCuts(referenceStreamCut, streamCut)
                .thenCompose(segments -> {
                    Map<Integer, List<Segment>> shards = segments.stream().collect(Collectors.groupingBy(x -> x.getNumber() / SHARD_SIZE));
                    return Futures.allOfWithResults(shards.entrySet().stream()
                            .map(entry -> getSealedSegmentSizeMapShard(entry.getKey())
                                    .thenApply(shardMap -> entry.getValue().stream().collect(Collectors.toMap(x -> x,
                                            x -> shardMap.getSize(x.segmentId())))))
                            .collect(Collectors.toList()))
                            .thenApply(listOfMap -> listOfMap.stream().flatMap(s -> s.entrySet().stream())
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                            .thenApply(sizes -> {
                                AtomicLong sizeTill = new AtomicLong(0L);
                                sizes.entrySet().forEach(x -> {
                                    Segment segment = x.getKey();
                                    // segments in both.. streamcut.offset - reference.offset
                                    if (streamCut.containsKey(segment.segmentId()) && referenceStreamCut.containsKey(segment.segmentId())) {
                                        sizeTill.addAndGet(streamCut.get(segment.segmentId()) - referenceStreamCut.get(segment.segmentId()));
                                    } else if (streamCut.containsKey(segment.segmentId())) {
                                        // segments only in streamcut: take their sizes in streamcut
                                        sizeTill.addAndGet(streamCut.get(segment.segmentId()));
                                    } else if (referenceStreamCut.containsKey(segment.segmentId())) {
                                        // segments only in reference: take their total size - offset in reference
                                        sizeTill.addAndGet(x.getValue() - referenceStreamCut.get(segment.segmentId()));
                                    } else {
                                        sizeTill.addAndGet(x.getValue());
                                    }
                                });
                                return sizeTill.get();
                            });
                });
    }

    private CompletableFuture<List<SegmentRecord>> segmentsBetweenStreamCuts(Map<SegmentRecord, Integer> mapFrom, Map<SegmentRecord, Integer> mapTo) {
        int toLow = Collections.min(mapFrom.values());
        int toHigh = Collections.max(mapFrom.values());
        int fromLow = Collections.min(mapTo.values());
        int fromHigh = Collections.max(mapTo.values());
        List<SegmentRecord> segments = new LinkedList<>();

        return fetchEpochs(fromLow, toHigh)
                .thenAccept(epochs -> {
                    epochs.forEach(epoch -> {
                        // for epochs that cleanly lie between from.high and to.low epochs we can include all segments present in them
                        // because they are guaranteed to be greater than `from` and less than `to` stream cuts.
                        if (epoch.getEpoch() >= fromHigh && epoch.getEpoch() <= toLow) {
                            segments.addAll(epoch.getSegments());
                        } else {
                            // for each segment in epoch.segments, find overlaps in from and to
                            epoch.getSegments().stream().filter(x -> !segments.contains(x)).forEach(segment -> {
                                // if segment.number >= from.segmentNumber && segment.number <= to.segmentNumber include segment.number
                                boolean greaterThanFrom = mapFrom.keySet().stream().filter(x -> x.overlaps(segment))
                                        .allMatch(x -> x.segmentId() <= segment.segmentId());
                                boolean lessThanTo = mapTo.keySet().stream().filter(x -> x.overlaps(segment))
                                        .allMatch(x -> segment.segmentId() <= x.segmentId());
                                if (greaterThanFrom && lessThanTo) {
                                    segments.add(segment);
                                }
                            });
                        }
                    });
                }).thenApply(x -> segments);
    }

    private CompletableFuture<Map<SegmentRecord, Integer>> computeEpochCutMap(Map<Long, Long> streamCut) {
        long mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        long oldest = streamCut.keySet().stream().min(Comparator.naturalOrder()).get();
        int epochLow = StreamSegmentNameUtils.getEpoch(oldest);
        int epochHigh = StreamSegmentNameUtils.getEpoch(mostRecent);

        return fetchEpochs(epochLow, epochHigh).thenApply(epochs ->  {
            List<Long> toFind = new ArrayList<>(streamCut.keySet());
            Map<SegmentRecord, Integer> resultSet = new HashMap<>();
            for (int i = epochHigh - epochLow - 1; i >= 0; i--) {
                if (toFind.isEmpty()) {
                    break;
                }
                EpochRecord epochRecord = epochs.get(i);
                List<Long> epochSegments = epochRecord.getSegments().stream().map(SegmentRecord::segmentId).collect(Collectors.toList());
                List<Long> found = toFind.stream().filter(epochSegments::contains).collect(Collectors.toList());
                resultSet.putAll(found.stream().collect(Collectors.toMap(x -> epochRecord.getSegments().stream()
                        .filter(z -> z.segmentId() == x).findFirst().get(), x -> epochRecord.getEpoch())));

                toFind.removeAll(epochSegments);
            }
            return resultSet;
        });
    }
    // endregion

    // region scale
    /**
     * This method attempts to start a new scale workflow. For this it first computes epoch transition and stores it in the metadastore.
     * This method can be called by manual scale or during the processing of auto-scale event. Which means there could be
     * concurrent calls to this method.
     *
     * @param segmentsToSeal segments that will be sealed at the end of this scale operation.
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param runOnlyIfStarted run only if the scale operation was started. This is set to true only for manual scale.
     * @return : list of newly created segments with current epoch
     */
    @Override
    public CompletableFuture<EpochTransitionRecord> startScale(final List<Long> segmentsToSeal,
                                                               final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                               final long scaleTimestamp,
                                                               boolean runOnlyIfStarted) {
        return verifyNotSealed()
                .thenCompose(verified -> getCurrentEpochRecord()
                        .thenCompose(currentEpoch -> {
                            if (!TableHelper.isScaleInputValid(segmentsToSeal, newRanges, currentEpoch)) {
                                log.error("scale input invalid {} {}", segmentsToSeal, newRanges);
                                throw new EpochTransitionOperationExceptions.InputInvalidException();
                            }

                            return startScale(segmentsToSeal, newRanges, scaleTimestamp, runOnlyIfStarted, currentEpoch);
                        }));
    }

    private CompletableFuture<EpochTransitionRecord> startScale(List<Long> segmentsToSeal, List<SimpleEntry<Double, Double>> newRanges,
                                                                long scaleTimestamp, boolean runOnlyIfStarted, EpochRecord currentEpoch) {
        return getEpochTransitionNode()
                .thenCompose(existing -> {
                    EpochTransitionRecord epochTransitionRecord = EpochTransitionRecord.parse(existing.getData());
                    // epoch transition should never be null. it may be empty or non empty.
                    if (!epochTransitionRecord.equals(EpochTransitionRecord.EMPTY)) {
                        // verify that its the same as the supplied input (--> segments to be sealed
                        // and new ranges are identical). else throw scale conflict exception
                        if (!TableHelper.verifyRecordMatchesInput(segmentsToSeal, newRanges, runOnlyIfStarted, epochTransitionRecord)) {
                            log.debug("scale conflict, another scale operation is ongoing");
                            throw new EpochTransitionOperationExceptions.ConflictException();
                        }
                        return CompletableFuture.completedFuture(epochTransitionRecord);
                    } else {
                        if (runOnlyIfStarted) {
                            log.info("scale not started, retry later.");
                            throw new TaskExceptions.StartException("Scale not started yet.");
                        }

                        // check input is valid and satisfies preconditions
                        if (!TableHelper.canScaleFor(segmentsToSeal, currentEpoch)) {
                            // invalid input, log and ignore
                            log.warn("scale precondition failed {}", segmentsToSeal);
                            throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                        }

                        EpochTransitionRecord epochTransition = TableHelper.computeEpochTransition(
                                currentEpoch, segmentsToSeal, newRanges, scaleTimestamp);

                        return updateEpochTransitionNode(new Data<>(epochTransition.toByteArray(), existing.getVersion()))
                                .handle((r, e) -> {
                                    if (Exceptions.unwrap(e) instanceof StoreException.WriteConflictException) {
                                        log.debug("scale conflict, another scale operation is ongoing");
                                        throw new EpochTransitionOperationExceptions.ConflictException();
                                    }

                                    log.info("scale for stream {}/{} accepted. Segments to seal = {}", scope, name,
                                            epochTransition.getSegmentsToSeal());
                                    return epochTransition;
                                });
                    }
                });
    }

    @Override
    public CompletableFuture<Void> scaleCreateNewEpoch(boolean isManualScale) {
        // check if epoch transition needs to be migrated for manual scale (in case rolling txn happened after the scale was submitted).
        // add new epoch record
        return checkState(state -> state.equals(State.SCALING))
                .thenCompose((Void v) -> getEpochTransitionNode().thenCompose(epochTransitionData -> {
                    EpochTransitionRecord epochTransition = EpochTransitionRecord.parse(epochTransitionData.getData());
                    if (isManualScale) {
                        // The epochTransitionNode is the barrier that prevents concurrent scaling.
                        // State is the barrier to ensure only one work happens at a time.
                        // However, if epochTransition node is created but before scaling happens,
                        // we can have rolling transaction kick in which would create newer epochs.
                        // For auto-scaling, the new duplicate epoch means the segment is sealed and no
                        // longer hot or cold.
                        // However for manual scaling, by virtue of accepting the request and creating
                        // new epoch transition record, we have promised the caller that we would scale
                        // to create sets of segments as requested by them.
                        return migrateManualScaleToNewEpoch(epochTransitionData);
                    } else {
                        return CompletableFuture.completedFuture(epochTransition);
                    }
                }).thenCompose(epochTransition -> getCurrentEpochRecord().thenCompose(currentEpoch -> {
                    // apply epoch transition on current epoch and compute new epoch
                    // use scale time in epoch transition record
                    List<SegmentRecord> newSegments = epochTransition.getNewSegmentsWithRange().entrySet().stream()
                            .map(x -> newSegmentRecord(x.getKey(), epochTransition.getTime(), x.getValue().getKey(), x.getValue().getKey()))
                            .collect(Collectors.toList());
                    EpochRecord epochRecord = getNewEpochRecord(currentEpoch, epochTransition.getNewEpoch(), epochTransition.getActiveEpoch(),
                            newSegments, epochTransition.getSegmentsToSeal(), epochTransition.getTime());

                    HistoryTimeSeriesRecord timeSeriesRecord = HistoryTimeSeriesRecord.builder().
                            epoch(epochTransition.getNewEpoch()).referenceEpoch(epochTransition.getNewEpoch())
                            .segmentsCreated(newSegments).segmentsSealed(Collections.emptyList()).creationTime(epochTransition.getTime()).build();
                    return createEpochRecord(epochRecord)
                            .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecord))
                            .thenCompose(x -> updateHistoryIndex(epochRecord.getEpoch(), epochRecord.getCreationTime()))
                            .thenCompose(x -> Futures.allOf(epochTransition.getSegmentsToSeal().stream()
                                    .map(segmentToSeal -> recordSegmentSealedEpoch(segmentToSeal, epochTransition.getNewEpoch()))
                                    .collect(Collectors.toList())));
                })));
    }

    private SegmentRecord newSegmentRecord(long segmentId, long time, Double low, Double high) {
        return newSegmentRecord(StreamSegmentNameUtils.getEpoch(segmentId), StreamSegmentNameUtils.getSegmentNumber(segmentId),
                time, low, high);
    }
    private SegmentRecord newSegmentRecord(int epoch, int segmentNumber, long time, Double low, Double high) {
        return SegmentRecord.builder().creationEpoch(epoch).segmentNumber(segmentNumber).creationTime(time)
                .keyStart(low).keyEnd(high).build();
    }

    private CompletableFuture<EpochTransitionRecord> migrateManualScaleToNewEpoch(Data<T> epochTransitionData) {
        EpochTransitionRecord epochTransition = EpochTransitionRecord.parse(epochTransitionData.getData());
        CompletableFuture<EpochRecord> activeEpochFuture = getCurrentEpochRecord();
        CompletableFuture<EpochRecord> epochRecordActiveEpochFuture = getEpochRecord(epochTransition.getActiveEpoch());

        return CompletableFuture.allOf(activeEpochFuture, epochRecordActiveEpochFuture)
                .thenCompose(x -> {
                    EpochRecord activeEpoch = activeEpochFuture.join();
                    EpochRecord epochRecordActiveEpoch = epochRecordActiveEpochFuture.join();
                    if (epochTransition.getActiveEpoch() == activeEpoch.getEpoch()) {
                        // no migration needed
                        return CompletableFuture.completedFuture(epochTransition);
                    } else if (activeEpoch.getEpoch() > epochTransition.getActiveEpoch() &&
                            activeEpoch.getReferenceEpoch() == epochRecordActiveEpoch.getReferenceEpoch()) {

                        List<Long> duplicateSegmentsToSeal = epochTransition.getSegmentsToSeal().stream()
                                .map(seg -> computeSegmentId(getSegmentNumber(seg), activeEpoch.getEpoch()))
                                .collect(Collectors.toList());

                        EpochTransitionRecord updatedRecord = TableHelper.computeEpochTransition(
                                activeEpoch, duplicateSegmentsToSeal, epochTransition.getNewSegmentsWithRange().values().asList(),
                                epochTransition.getTime());
                        return updateEpochTransitionNode(new Data<>(updatedRecord.toByteArray(), epochTransitionData.getVersion()))
                                .thenApply(v -> updatedRecord);
                    } else {
                        // we should never reach here!! rescue and exit
                        return updateEpochTransitionNode(new Data<>(EpochTransitionRecord.EMPTY.toByteArray(), epochTransitionData.getVersion()))
                                .thenCompose(v -> resetStateConditionally(State.SCALING))
                                .thenApply(v -> {
                                    log.warn("Scale epoch transition record is inconsistent with data in the table. {}",
                                            epochTransition.getNewEpoch());
                                    throw new IllegalStateException("Epoch transition record is inconsistent.");
                                });
                    }
                });
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     *
     * @param sealedSegmentSizes sealed segments with sizes
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> completeScale(Map<Long, Long> sealedSegmentSizes) {
        // update the size of sealed segments
        // update current epoch record
        return checkState(state -> state.equals(State.SCALING))
                .thenCompose(v -> getEpochTransitionNode()
                        .thenCompose(epochTransitionData -> {
                            EpochTransitionRecord epochTransition = EpochTransitionRecord.parse(epochTransitionData.getData());
                            return Futures.toVoid(clearMarkers(epochTransition.getSegmentsToSeal())
                                    .thenCompose(x -> updateSealedSegmentSizes(sealedSegmentSizes))
                                    .thenCompose(newEpochRecord -> updateCurrentEpochRecord(epochTransition.getNewEpoch())))
                                    .thenCompose(r -> updateEpochTransitionNode(new Data<>(EpochTransitionRecord.EMPTY.toByteArray(),
                                            epochTransitionData.getVersion())));
                        }));
    }
    // endregion

    // region cold marker
    @Override
    public CompletableFuture<Void> setColdMarker(long segmentId, long timestamp) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentId)).thenCompose(x -> {
            if (x != null) {
                byte[] b = new byte[Long.BYTES];
                BitConverter.writeLong(b, 0, timestamp);
                final Data<T> data = new Data<>(b, x.getVersion());
                return updateMarkerData(segmentId, data);
            } else {
                return createMarkerData(segmentId, timestamp);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getColdMarker(long segmentId) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentId))
                .thenApply(x -> (x != null) ? BitConverter.readLong(x.getData(), 0) : 0L);
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(long segmentId) {
        return verifyLegalState().thenCompose(v -> removeMarkerData(segmentId));
    }

    private CompletableFuture<Void> clearMarkers(final Set<Long> segments) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                .map(this::removeColdMarker).collect(Collectors.toList())));
    }
    // endregion

    //region rolling txn
    @Override
    public CompletableFuture<Void> startRollingTxn(int transactionEpoch, int activeEpoch) {
        return getCommittingTxnRecord().thenCompose(committingTxnRecordData -> {
            CommittingTransactionsRecord record = CommittingTransactionsRecord.parse(committingTxnRecordData.getData());
            assert(record.getEpoch() == transactionEpoch);
            if (activeEpoch == record.getActiveEpoch()) {
                return CompletableFuture.completedFuture(null);
            } else {
                CommittingTransactionsRecord update = CommittingTransactionsRecord.startRollingTxn(record, activeEpoch);
                return updateCommittingTxnRecord(new Data<>(update.toByteArray(), committingTxnRecordData.getVersion()));
            }
        });
    }

    @Override
    public CompletableFuture<Void> rollingTxnCreateNewEpochs(Map<Long, Long> sealedTxnEpochSegments, long time) {
        return checkState(state -> state.equals(State.COMMITTING_TXN) || state.equals(State.SEALING))
            .thenCompose(v -> getCommittingTransactionsRecord()
                .thenCompose(committingTransactionsRecord -> getActiveEpoch(true)
                .thenCompose(activeEpochRecord -> getEpochRecord(committingTransactionsRecord.getEpoch())
                        .thenCompose(transactionEpochRecord -> {
                            if (activeEpochRecord.getEpoch() > committingTransactionsRecord.getActiveEpoch()) {
                                log.debug("Duplicate Epochs {} already created. Ignore.", committingTransactionsRecord.getActiveEpoch() + 2);
                                return CompletableFuture.completedFuture(null);
                            }
                            List<SegmentRecord> duplicateTxnSegments = transactionEpochRecord.getSegments().stream()
                                    .map(x -> newSegmentRecord(computeSegmentId(getSegmentNumber(x.segmentId()),
                                            activeEpochRecord.getEpoch() + 1), time, x.getKeyStart(), x.getKeyEnd()))
                                    .collect(Collectors.toList());
                            List<SegmentRecord> duplicateActiveSegments = transactionEpochRecord.getSegments().stream()
                                    .map(x -> newSegmentRecord(computeSegmentId(getSegmentNumber(x.segmentId()),
                                            activeEpochRecord.getEpoch() + 1), time, x.getKeyStart(), x.getKeyEnd()))
                                    .collect(Collectors.toList());

                            EpochRecord duplicateTxnEpoch = EpochRecord.builder().epoch(activeEpochRecord.getEpoch() + 1)
                                    .referenceEpoch(transactionEpochRecord.getReferenceEpoch()).segments(duplicateTxnSegments)
                                    .creationTime(time).build();

                            EpochRecord duplicateActiveEpoch = EpochRecord.builder().epoch(activeEpochRecord.getEpoch() + 2)
                                    .referenceEpoch(activeEpochRecord.getReferenceEpoch()).segments(duplicateActiveSegments)
                                    .creationTime(time).build();

                            HistoryTimeSeriesRecord timeSeriesRecordTxnEpoch = HistoryTimeSeriesRecord.builder().epoch(duplicateTxnEpoch.getEpoch())
                                    .referenceEpoch(duplicateTxnEpoch.getReferenceEpoch()).creationTime(time).build();
                            HistoryTimeSeriesRecord timeSeriesRecordActiveEpoch = HistoryTimeSeriesRecord.builder().epoch(duplicateActiveEpoch.getEpoch())
                                    .referenceEpoch(duplicateActiveEpoch.getReferenceEpoch()).creationTime(time).build();
                            return createEpochRecord(duplicateTxnEpoch)
                                    .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordTxnEpoch))
                                    .thenCompose(x -> updateHistoryIndex(duplicateTxnEpoch.getEpoch(), time))
                                    .thenCompose(x -> createEpochRecord(duplicateActiveEpoch))
                                    .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordActiveEpoch))
                                    .thenCompose(x -> updateHistoryIndex(duplicateActiveEpoch.getEpoch(), time))
                                    .thenCompose(x -> Futures.allOf(activeEpochRecord.getSegments().stream().map(segment ->
                                            recordSegmentSealedEpoch(segment.segmentId(), duplicateTxnEpoch.getEpoch())).collect(Collectors.toList())))
                                    .thenCompose(x -> Futures.allOf(duplicateTxnEpoch.getSegments().stream().map(segment ->
                                            recordSegmentSealedEpoch(segment.segmentId(), duplicateActiveEpoch.getEpoch())).collect(Collectors.toList())));
                        })
                        .thenCompose(r -> updateSealedSegmentSizes(sealedTxnEpochSegments)))));
    }

    @Override
    public CompletableFuture<Void> completeRollingTxn(Map<Long, Long> sealedActiveEpochSegments) {
        return checkState(state -> state.equals(State.COMMITTING_TXN) || state.equals(State.SEALING))
                .thenCompose(v -> getActiveEpoch(true)
                .thenCompose(activeEpochRecord -> getCommittingTransactionsRecord()
                        .thenCompose(committingTxnRecord -> {
                            int activeEpoch = committingTxnRecord.getActiveEpoch();
                            if (activeEpochRecord.getEpoch() == activeEpoch) {
                                return updateSealedSegmentSizes(sealedActiveEpochSegments)
                                        .thenCompose(x -> clearMarkers(sealedActiveEpochSegments.keySet()))
                                        .thenCompose(x -> updateCurrentEpochRecord(activeEpoch + 2));
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        })));
    }
    //endregion

    // region transactions
    @Override
    public CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit) {
        return getActiveEpoch(false)
                .thenApply(epochRecord -> {
                    // always set transaction epoch as refrence epoch so that all transactions on duplicate epochs
                    // are collected.
                    // epochs that are not duplicates will refer to themselves.
                    long msb64Bit = (long) epochRecord.getReferenceEpoch() << 32 | msb32Bit & 0xFFFFFFFFL;
                    return new UUID(msb64Bit, lsb64Bit);
                });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime) {
        final long current = System.currentTimeMillis();
        final long leaseTimestamp = current + lease;
        final long maxExecTimestamp = current + maxExecutionTime;
        // extract epoch from txnid
        final int epoch = getTransactionEpoch(txnId);
        return verifyLegalState().thenCompose(v -> createNewTransaction(txnId, current, leaseTimestamp, maxExecTimestamp))
                .thenApply(v -> new VersionedTransactionData(epoch, txnId, 0, TxnStatus.OPEN, current,
                        current + maxExecutionTime));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData,
                                                                       final long lease) {
        // Update txn record with new lease value and return versioned tx data.
        final int epoch = txnData.getEpoch();
        final UUID txnId = txnData.getId();
        final int version = txnData.getVersion();
        final long creationTime = txnData.getCreationTime();
        final long maxExecutionExpiryTime = txnData.getMaxExecutionExpiryTime();
        final TxnStatus status = txnData.getStatus();
        final ActiveTxnRecord newData = new ActiveTxnRecord(creationTime, System.currentTimeMillis() + lease,
                maxExecutionExpiryTime, status);
        final Data<Integer> data = new Data<>(newData.toByteArray(), version);

        return updateActiveTx(epoch, txnId, data).thenApply(x -> new VersionedTransactionData(epoch, txnId,
                version + 1, status, creationTime, maxExecutionExpiryTime));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return getActiveTx(epoch, txId)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = ActiveTxnRecord.parse(data.getData());
                    return new VersionedTransactionData(epoch, txId, data.getVersion(),
                            activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                            activeTxnRecord.getMaxExecutionExpiryTime());
                });
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return verifyLegalState().thenCompose(v -> getActiveTx(epoch, txId).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ActiveTxnRecord.parse(ok.getData()).getTxnStatus();
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.UNKNOWN)) {
                return getCompletedTxnStatus(txId);
            } else {
                return CompletableFuture.completedFuture(x);
            }
        }));
    }

    private CompletableFuture<TxnStatus> getCompletedTxnStatus(UUID txId) {
        return getCompletedTx(txId).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return CompletedTxnRecord.parse(ok.getData()).getCompletionStatus();
        });
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final UUID txId, final boolean commit,
                                                                              final Optional<Integer> version) {
        val legal = verifyLegalState();
        int epoch = getTransactionEpoch(txId);
        return legal.thenCompose(v -> sealActiveTxn(epoch, txId, commit, version))
                .exceptionally(ex -> new SimpleEntry<>(handleDataNotFoundException(ex), null))
                .thenCompose(pair -> {
                    if (pair.getKey() == TxnStatus.UNKNOWN) {
                        return validateCompletedTxn(txId, commit, "seal").thenApply(status -> new SimpleEntry<>(status, null));
                    } else {
                        return CompletableFuture.completedFuture(pair);
                    }
                });
    }

    /**
     * Seal a transaction in OPEN/COMMITTING_TXN/ABORTING state. This method does CAS on the transaction data node if
     * the transaction is in OPEN state, optionally checking version of transaction data node, if required.
     *
     * @param epoch   transaction epoch.
     * @param txId    transaction identifier.
     * @param commit  boolean indicating whether to commit or abort the transaction.
     * @param version optional expected version of transaction node to validate before updating it.
     * @return        a pair containing transaction status and its epoch.
     */
    private CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealActiveTxn(final int epoch,
                                                                             final UUID txId,
                                                                             final boolean commit,
                                                                             final Optional<Integer> version) {
        return getActiveTx(epoch, txId).thenCompose(data -> {
            ActiveTxnRecord txnRecord = ActiveTxnRecord.parse(data.getData());
            int dataVersion = version.orElseGet(data::getVersion);
            TxnStatus status = txnRecord.getTxnStatus();
            switch (status) {
                case OPEN:
                    return sealActiveTx(epoch, txId, commit, txnRecord, dataVersion).thenApply(y ->
                            new SimpleEntry<>(commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING, epoch));
                case COMMITTING:
                case COMMITTED:
                    if (commit) {
                        return CompletableFuture.completedFuture(new SimpleEntry<>(status, epoch));
                    } else {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " Transaction: " + txId.toString() +
                                        " State: " + status.name());
                    }
                case ABORTING:
                case ABORTED:
                    if (commit) {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " +
                                        status.name());
                    } else {
                        return CompletableFuture.completedFuture(new SimpleEntry<>(status, epoch));
                    }
                default:
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                            "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        });
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return checkState(state -> state.equals(State.COMMITTING_TXN) || state.equals(State.SEALING))
                .thenCompose(v -> checkTransactionStatus(txId))
                .thenApply(x -> {
                    switch (x) {
                        // Only sealed transactions can be committed
                        case COMMITTED:
                        case COMMITTING:
                            return x;
                        case OPEN:
                        case ABORTING:
                        case ABORTED:
                            throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                    "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + x.toString());
                        case UNKNOWN:
                        default:
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                                    "Stream: " + getName() + " Transaction: " + txId.toString());
                    }
                }).thenCompose(x -> {
                    if (x.equals(TxnStatus.COMMITTING)) {
                        return createCompletedTxEntry(txId, TxnStatus.COMMITTED, System.currentTimeMillis());
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                }).thenCompose(x -> removeActiveTxEntry(epoch, txId)).thenApply(x -> TxnStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return verifyLegalState().thenCompose(v -> checkTransactionStatus(txId)).thenApply(x -> {
            switch (x) {
                case ABORTING:
                case ABORTED:
                    return x;
                case OPEN:
                case COMMITTING:
                case COMMITTED:
                    throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                            "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + x.name());
                case UNKNOWN:
                default:
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                            "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.ABORTING)) {
                return createCompletedTxEntry(txId, TxnStatus.ABORTED, System.currentTimeMillis());
            } else {
                return CompletableFuture.completedFuture(null); // already aborted, do nothing
            }
        }).thenCompose(y -> removeActiveTxEntry(epoch, txId)).thenApply(y -> TxnStatus.ABORTED);
    }

    @SneakyThrows
    private TxnStatus handleDataNotFoundException(Throwable ex) {
        if (Exceptions.unwrap(ex) instanceof DataNotFoundException) {
            return TxnStatus.UNKNOWN;
        } else {
            throw ex;
        }
    }

    private CompletableFuture<TxnStatus> validateCompletedTxn(UUID txId, boolean commit, String operation) {
        return getCompletedTxnStatus(txId).thenApply(status -> {
            if ((commit && status == TxnStatus.COMMITTED) || (!commit && status == TxnStatus.ABORTED)) {
                return status;
            } else if (status == TxnStatus.UNKNOWN) {
                throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString());
            } else {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + status.name());
            }
        });
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return verifyLegalState().thenCompose(v -> getCurrentTxns())
                .thenApply(x -> x.entrySet()
                        .stream()
                        .collect(toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxnRecord.parse(v.getValue().getData()))));
    }

    @Override
    public CompletableFuture<Void> createCommittingTransactionsRecord(final int epoch, final List<UUID> txnsToCommit) {
        return createCommittingTxnRecord(new CommittingTransactionsRecord(epoch, txnsToCommit).toByteArray());
    }

    @Override
    public CompletableFuture<CommittingTransactionsRecord> getCommittingTransactionsRecord() {
        CompletableFuture<CommittingTransactionsRecord> result = new CompletableFuture<>();
        getCommittingTxnRecord()
                .whenComplete((r, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(e);
                        }
                    } else {
                        result.complete(CommittingTransactionsRecord.parse(r.getData()));
                    }
                });
        return result;
    }

    @Override
    public CompletableFuture<Void> resetCommittingTransactionsRecord() {
        return getCommittingTxnRecord()
                .thenCompose(data -> updateCommittingTxnRecord(new Data<>(CommittingTransactionsRecord.EMPTY.toByteArray(), data.getVersion())));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(final int epoch) {
        return getTxnInEpoch(epoch)
                .thenApply(x -> x.entrySet()
                        .stream()
                        .collect(toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxnRecord.parse(v.getValue().getData()))));
    }

    int getTransactionEpoch(UUID txId) {
        // epoch == UUID.msb >> 32
        return TableHelper.getTransactionEpoch(txId);
    }
    // endregion

    // region retention
    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord streamCut) {
        return getRetentionSetData()
                .thenCompose(data -> {
                    RetentionSet retention = RetentionSet.parse(data.getData());

                    RetentionSet update = RetentionSet.addStreamCutIfLatest(retention, streamCut);
                    return createStreamCutRecordData(streamCut)
                            .thenCompose(v -> updateRetentionSetData(new Data<>(update.toByteArray(), data.getVersion())));
                });
    }

    @Override
    public CompletableFuture<RetentionSet> getRetentionSet() {
        return getRetentionSetData()
                .thenApply(data -> RetentionSet.parse(data.getData()));
    }

    @Override
    public CompletableFuture<StreamCutRecord> getStreamCutRecord(RetentionSetRecord record) {
        return getStreamCutRecordData(record.getRecordingTime()).thenApply(x -> StreamCutRecord.parse(x.getData()));
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(RetentionSetRecord record) {
        return getRetentionSetData()
                .thenCompose(data -> {
                    RetentionSet retention = RetentionSet.parse(data.getData());
                    RetentionSet update = RetentionSet.removeStreamCutBefore(retention, record);
                    List<RetentionSetRecord> toRemove = retention.retentionRecordsBefore(record);
                    return Futures.allOf(toRemove.stream().map(x -> deleteStreamCutRecordData(x.getRecordingTime())).collect(Collectors.toList()))
                            .thenCompose(x -> updateRetentionSetData(new Data<>(update.toByteArray(), data.getVersion())));
                });
    }
    // endregion

    // region waiting request processor
    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName) {
        return createWaitingRequestNodeIfAbsent(processorName.getBytes());
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor() {
        return getWaitingRequestNode()
                .handle((data, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    } else {
                        return new String(data.getData());
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName) {
        return getWaitingRequestProcessor()
                .thenCompose(waitingRequest -> {
                    if (waitingRequest != null && waitingRequest.equals(processorName)) {
                        return deleteWaitingRequestNode();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }
    // endregion

    // region abstract methods
    //region create delete
    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration,
                                                                       final long creationTime, final int startingSegmentNumber);

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> deleteStream();
    // endregion

    // region configuration
    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord data);

    abstract CompletableFuture<Void> setConfigurationData(final Data<T> configuration);

    abstract CompletableFuture<Data<T>> getConfigurationData(boolean ignoreCached);
    // endregion

    // region truncation
    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncation);

    abstract CompletableFuture<Void> setTruncationData(final Data<T> truncationRecord);

    abstract CompletableFuture<Data<T>> getTruncationData(boolean ignoreCached);
    // endregion

    // region state
    abstract CompletableFuture<Void> createStateIfAbsent(final State state);

    abstract CompletableFuture<Void> setStateData(final Data<T> state);

    abstract CompletableFuture<Data<T>> getStateData(boolean ignoreCached);
    // endregion

    // region retention
    abstract CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data);

    abstract CompletableFuture<Void> createStreamCutRecordData(StreamCutRecord record);

    abstract CompletableFuture<Data<T>> getStreamCutRecordData(long recordingTime);

    abstract CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime);

    abstract CompletableFuture<Void> updateRetentionSetData(Data<T> tData);

    abstract CompletableFuture<Data<T>> getRetentionSetData();
    // endregion

    // region history
    abstract CompletableFuture<Void> createHistoryTimeIndexRootIfAbsent(byte[] data);
    abstract CompletableFuture<Data<T>> getHistoryTimeIndexRootNodeData();
    abstract CompletableFuture<Void> updateHistoryIndexRootData(final Data<T> updated);

    abstract CompletableFuture<Void> createHistoryTimeIndexLeafDataIfAbsent(int leafChunkNumber, byte[] data);
    abstract CompletableFuture<Void> updateHistoryTimeIndexLeafData(int historyLeaf, Data<T> tData);
    abstract CompletableFuture<Data<T>> getHistoryIndexLeafData(int leaf, boolean ignoreCached);

    abstract CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data);
    abstract CompletableFuture<Data<T>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached);
    abstract CompletionStage<Void> updateHistoryTimeSeriesChunkData(int historyChunk, Data<T> tData);

    abstract CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data);
    abstract CompletableFuture<Void> updateCurrentEpochRecordData(Data<T> data);
    abstract CompletableFuture<Data<T>> getCurrentEpochRecordData(boolean ignoreCached);

    abstract CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data);
    abstract CompletableFuture<Data<T>> getEpochRecordData(int epoch);

    abstract CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, byte[] data);
    abstract CompletableFuture<Data<T>> getSealedSegmentSizesMapShardData(int shard);
    abstract CompletableFuture<Void> updateSealedSegmentSizesMapShardData(int shard, Data<T> data);

    abstract CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch);
    abstract CompletableFuture<Data<T>> getSegmentSealedRecordData(long segmentId);
    // endregion

    // region transactions
    abstract CompletableFuture<Void> createNewTransaction(final UUID txId,
                                                          final long timestamp,
                                                          final long leaseExpiryTime,
                                                          final long maxExecutionExpiryTime);

    abstract CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> updateActiveTx(final int epoch,
                                                    final UUID txId,
                                                    final Data<Integer> data);

    abstract CompletableFuture<Void> sealActiveTx(final int epoch,
                                                  final UUID txId, final boolean commit,
                                                  final ActiveTxnRecord txnRecord,
                                                  final int version);

    abstract CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId);

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Map<String, Data<T>>> getTxnInEpoch(int epoch);
    // endregion

    // region marker
    abstract CompletableFuture<Void> createMarkerData(long segmentId, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(long segmentId, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(long segmentId);

    abstract CompletableFuture<Data<T>> getMarkerData(long segmentId);
    // endregion

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;

    // region scale
    abstract CompletableFuture<Void> createEpochTransitionNode(byte[] epochTransition);

    abstract CompletableFuture<Void> updateEpochTransitionNode(Data<T> epochTransition);

    abstract CompletableFuture<Data<T>> getEpochTransitionNode();
    // endregion

    // region txn commit
    abstract CompletableFuture<Void> createCommittingTxnRecord(byte[] committingTxns);

    abstract CompletableFuture<Data<T>> getCommittingTxnRecord();

    abstract CompletableFuture<Void> updateCommittingTxnRecord(Data<T> data);
    // endregion

    // region processor
    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data);

    abstract CompletableFuture<Data<T>> getWaitingRequestNode();

    abstract CompletableFuture<Void> deleteWaitingRequestNode();
    // endregion
    // endregion
}
