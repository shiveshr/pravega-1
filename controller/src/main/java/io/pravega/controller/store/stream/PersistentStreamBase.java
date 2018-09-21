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
import io.pravega.controller.store.stream.tables.HistoryIndexLeaf;
import io.pravega.controller.store.stream.tables.HistoryIndexRecord;
import io.pravega.controller.store.stream.tables.HistoryIndexRootNode;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.HistoryTimeSeries;
import io.pravega.controller.store.stream.tables.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.tables.RetentionSet;
import io.pravega.controller.store.stream.tables.RetentionSetRecord;
import io.pravega.controller.store.stream.tables.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.shared.segment.StreamSegmentNameUtils;
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
public abstract class PersistentStreamBase<T> implements Stream {

    // TODO: shivesh move it to constant
    public static final int SHARD_SIZE = 10000;
    public static final int HISTORY_CHUNK_SIZE = 10000;
    private final String scope;
    private final String name;

    PersistentStreamBase(final String scope, final String name) {
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
        // TODO: shivesh create stream
        // create configuration
        // create truncation
        // create state
        // create epoch 0
        // create current epoch
        //
        // compute what epoch 0 will look like.. create epoch 0.
        // add epoch 0 with creation time to the index
        // add time -> index 0 in top level index

        return checkScopeExists()
                .thenCompose((Void v) -> checkStreamExists(configuration, createTimestamp, startingSegmentNumber))
                .thenCompose(createStreamResponse -> createConfigurationIfAbsent(
                        StreamConfigurationRecord.complete(createStreamResponse.getConfiguration()))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamTruncationRecord.EMPTY))
                        .thenCompose((Void v) -> createStateIfAbsent(State.CREATING))
                        .thenCompose((Void v) -> {
                            final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
                            // create epoch 0 record
                            final double keyRangeChunk = 1.0 / numSegments;

                            long creationTime = createStreamResponse.getTimestamp();
                            final List<Segment> segments = IntStream.range(0, numSegments)
                                    .boxed()
                                    .map(x -> new Segment(0, startingSegmentNumber + x,
                                            creationTime, x * keyRangeChunk, (x + 1) * keyRangeChunk))
                                    .collect(Collectors.toList());

                            HistoryRecord epoch0 = HistoryRecord.builder().epoch(0).referenceEpoch(0).segments(segments)
                                    .creationTime(creationTime).build();

                            return createEpochRecord(epoch0)
                                    .thenCompose(r -> createHistoryIndex(creationTime))
                                    .thenCompose(r -> {
                                        HistoryTimeSeriesRecord record = HistoryTimeSeriesRecord.builder().epoch(0).referenceEpoch(0)
                                                .segmentsCreated(epoch0.getSegments()).segmentsSealed(Collections.emptyList()).creationTime(epoch0.getScaleTime()).build();
                                        return createHistoryTimeSeriesChunk(0, record);
                                    })
                                    .thenCompose(r -> createSegmentSealedChunk(0))
                                    .thenCompose(r -> createRetentionSet())
                                    .thenCompose(r -> createCurrentEpochRecord(epoch0));
                        })
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletableFuture<Void> createRetentionSet() {
        RetentionSet set = new RetentionSet(Collections.emptyList());
        return createRetentionSetData(new Data<>(set.toByteArray(), 0));
    }

    private CompletableFuture<Void> createSegmentSealedChunk(int shardNumber) {
        SealedSegmentsMapShard shard = new SealedSegmentsMapShard(shardNumber, Collections.emptyMap());
        return createSegmentSealedChunkData(shardNumber, shard.toByteArray());
    }

    CompletableFuture<Void> createHistoryTimeSeriesChunk(int chunkNumber, HistoryTimeSeriesRecord epoch) {
        HistoryTimeSeries timeSeries = HistoryTimeSeries.builder().build();
        HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(timeSeries, epoch);
        return createHistoryTimeSeriesChunkData(chunkNumber, update.toByteArray());
    }

    private CompletableFuture<Void> createCurrentEpochRecord(HistoryRecord epoch) {
        return createCurrentEpochRecordData(epoch.toByteArray());
    }

    private CompletableFuture<Void> createHistoryIndex(long time) {
        return createHistoryIndexRoot(new HistoryIndexRootNode(Lists.newArrayList(time)).toByteArray())
                .thenCompose(v -> {
                    HistoryIndexRecord record = new HistoryIndexRecord(time, 0);
                    HistoryIndexLeaf historyIndexLeaf = new HistoryIndexLeaf(Lists.newArrayList(record));
                    return createHistoryLeaf(0, historyIndexLeaf.toByteArray());
                });
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
    }
    // endregion

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
                                                                              Map<Segment, Integer> epochCutMap) {
        log.debug("computing truncation for stream {}/{}", scope, name);
        // compute segments to delete between previous and streamcut.

        // find segments between "previous" stream cut and current stream cut. these are segments to delete.
        // Note: exclude segments in current streamcut
        return segmentsBetweenStreamCuts(previous.getCutEpochMap(), epochCutMap)
                .thenApply(segmentsBetween -> {
                    // toDelete =
                    // all segments in between
                    // all segments from previous that are not present in streamCut
                    List<Long> toDelete = segmentsBetween.stream().filter(x -> !streamCut.containsKey(x))
                            .map(Segment::segmentId).collect(Collectors.toList());

                    return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                            previous.getDeletedSegments(), ImmutableSet.copyOf(toDelete), true);
                });
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

    @Override
    public CompletableFuture<StreamTruncationRecord> getTruncationRecord(boolean ignoreCached) {
        return getTruncationData(ignoreCached)
                .thenApply(data -> data == null ? StreamTruncationRecord.EMPTY : StreamTruncationRecord.parse(data.getData()));
    }

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
                    Optional<Segment> segmentWithRange = epochRecord.getSegments().stream()
                            .filter(x -> x.segmentId() == segmentId).findAny();
                    if (segmentWithRange.isPresent()) {
                        return new Segment(segmentWithRange.get().segmentId(), epochRecord.getScaleTime(),
                                segmentWithRange.get().getKeyStart(), segmentWithRange.get().getKeyEnd());
                    } else {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "segment not found in epoch");
                    }
                });
    }

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
                    CompletableFuture<List<HistoryRecord>> records = CompletableFuture.allOf(fromEpoch, toEpoch)
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

    private CompletableFuture<List<HistoryRecord>> fetchEpochs(int fromEpoch, int toEpoch) {
        // TODO: shivesh verify reduce function!!!
        // fetch history time series chunk corresponding to from.
        // read entries till either last entry or till to
        // if to is not in this chunk fetch the next chunk and read till to
        // keep doing this until all records till to have been read.
        List<HistoryRecord> records;
        // keep computing history record from history time series by applying delta on previous.

        List<CompletableFuture<ArrayList<HistoryRecord>>> chunks = IntStream.range(fromEpoch / HISTORY_CHUNK_SIZE, toEpoch / HISTORY_CHUNK_SIZE).mapToObj(i -> {
            int firstEpoch = i * HISTORY_CHUNK_SIZE > fromEpoch ? i * HISTORY_CHUNK_SIZE : fromEpoch;
            return getEpochRecord(firstEpoch)
                    .thenCompose(first -> getHistoryTimeSeriesChunk(i)
                            .thenApply(x -> {
                                ArrayList<HistoryRecord> identity = Lists.newArrayList(first);

                                return x.getHistoryRecords().stream().filter(r -> r.getEpoch() > fromEpoch && r.getEpoch() <= toEpoch)
                                        .reduce(identity, (r, s) -> {
                                            HistoryRecord next = getNewEpochRecord(r.get(r.size() - 1), s.getEpoch(), s.getReferenceEpoch(),
                                                    s.getSegmentsCreated(), s.getSegmentsSealed().stream().map(Segment::segmentId)
                                                            .collect(Collectors.toList()), s.getScaleTime());
                                            return Lists.newArrayList(next);
                                        }, (r, s) -> {
                                            ArrayList<HistoryRecord> list = new ArrayList<>(r);
                                            list.addAll(s);
                                            return list;
                                        });
                            }));
        }).collect(Collectors.toList());

        return Futures.allOfWithResults(chunks).thenApply(c -> c.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    private CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber) {
        return getHistoryTimeSeriesChunkData(chunkNumber)
                .thenApply(x -> HistoryTimeSeries.parse(x.getData()));
    }

    private List<ScaleMetadata> mapToScaleMetadata(List<HistoryRecord> historyRecords) {
        final AtomicReference<List<Segment>> previous = new AtomicReference<>();
        return historyRecords.stream()
                .map(record -> {
                    long splits = 0;
                    long merges = 0;
                    List<Segment> segments = record.getSegments();
                    if (previous.get() != null) {
                        splits = findSegmentSplitsMerges(previous.get(), segments);
                        merges = findSegmentSplitsMerges(segments, previous.get());
                    }
                    previous.set(segments);
                    return new ScaleMetadata(record.getScaleTime(), segments, splits, merges);
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
    private long findSegmentSplitsMerges(List<Segment> referenceSegmentsList, List<Segment> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }

    @Override
    public CompletableFuture<Map<Segment, List<Long>>> getSuccessorsWithPredecessors(final long segmentId) {
        // get segment sealed record.
        // fetch segment sealed record.
        return getSegmentSealedRecord(segmentId)
                .thenCompose(sealedEpoch -> {
                    // if sealed record exists. fetch its sealing epoch.
                    // Note: sealed record is created even before the segment is sealed. So if client is requesting for successor,
                    // we should find it.
                    CompletableFuture<HistoryRecord> sealedEpochFuture = getEpochRecord(sealedEpoch);

                    // fetch previous epoch as well.
                    CompletableFuture<HistoryRecord> previousEpochFuture = getEpochRecord(sealedEpoch - 1);

                    return CompletableFuture.allOf(sealedEpochFuture, previousEpochFuture)
                            .thenApply(x -> {
                                HistoryRecord sealedEpochRecord = sealedEpochFuture.join();
                                HistoryRecord previousEpochRecord = previousEpochFuture.join();
                                Optional<Segment> segmentOpt = previousEpochRecord.getSegments().stream()
                                        .filter(r -> r.segmentId() == segmentId).findAny();
                                assert (segmentOpt.isPresent());
                                Segment segment = segmentOpt.get();

                                List<Segment> successors = sealedEpochRecord.getSegments().stream()
                                        .filter(r -> r.overlaps(segment)).collect(Collectors.toList());

                                return successors.stream()
                                        .collect(Collectors.toMap(z -> z, z -> previousEpochRecord.getSegments()
                                                .stream().filter(predecessor -> predecessor.overlaps(z))
                                                .map(Segment::segmentId).collect(Collectors.toList())));
                            });
                });
    }

    private CompletableFuture<Integer> getSegmentSealedRecord(long segmentId) {
        return getSegmentSealedRecordData(segmentId).thenApply(x -> BitConverter.readInt(x.getData(), 0));
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments() {
        // read current epoch record
        return getCurrentEpochRecord().thenApply(HistoryRecord::getSegments);
    }

    private CompletableFuture<HistoryRecord> getCurrentEpochRecord() {
        return getCurrentEpochRecordData().thenApply(x -> HistoryRecord.parse(x.getData()));
    }

    /**
     *
     * @param timestamp point in time.
     * @return : list of active segment numbers at given time stamp
     */
    @Override
    public CompletableFuture<Map<Segment, Long>> getActiveSegments(final long timestamp) {
        return getTruncationRecord(true)
                .thenCompose(truncationRecord -> findEpochAtTime(timestamp)
                        .thenCompose(this::getEpochRecord)
                        .thenApply(epochRecord -> TableHelper.getActiveSegments(epochRecord, truncationRecord)));
    }

    private CompletableFuture<Integer> findEpochAtTime(long timestamp) {
        // fetch history index BTree root and find the leaf node corresponding to given time.
        // fetch the leaf node and perform binary search in the leaf node to find the epoch corresponding to given time
        return getHistoryIndexRootNode()
                .thenApply(root -> root.findLeafNode(timestamp))
                .thenCompose(this::getHistoryIndexLeaf)
                .thenApply(leafNode -> leafNode.find(timestamp))
                .thenApply(HistoryIndexRecord::getEpoch);
    }

    private CompletableFuture<HistoryIndexLeaf> getHistoryIndexLeaf(int leaf) {
        return getHistoryIndexLeafData(leaf).thenApply(x -> HistoryIndexLeaf.parse(x.getData()));
    }

    private CompletableFuture<HistoryIndexRootNode> getHistoryIndexRootNode() {
        return getHistoryIndexRootNodeData().thenApply(x -> HistoryIndexRootNode.parse(x.getData()));
    }

    @Override
    public CompletableFuture<List<Segment>> getSegmentsInEpoch(final int epoch) {
        // read history record for the epoch
        return getEpochRecord(epoch).thenApply(HistoryRecord::getSegments);
    }

    @Override
    public CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to) {
        // compute epoch cut map for from till to
        // if from is empty we need to start from epoch 0.
        // if to is empty we need to go on till current epoch.
        CompletableFuture<Map<Segment, Integer>> mapFromFuture = from.isEmpty() ?
                getEpochRecord(0).thenApply(epoch -> epoch.getSegments().stream().collect(Collectors.toMap(x -> x, x -> epoch.getEpoch())))
                : computeEpochCutMap(from);
        CompletableFuture<Map<Segment, Integer>> mapToFuture = to.isEmpty() ?
                getCurrentEpochRecord().thenApply(epoch -> epoch.getSegments().stream().collect(Collectors.toMap(x -> x, x -> epoch.getEpoch())))
                : computeEpochCutMap(to);

        return CompletableFuture.allOf(mapFromFuture, mapToFuture)
            .thenCompose(x -> segmentsBetweenStreamCuts(mapFromFuture.join(), mapToFuture.join()));
    }

    private CompletableFuture<List<Segment>> segmentsBetweenStreamCuts(Map<Segment, Integer> mapFrom, Map<Segment, Integer> mapTo) {
        int toLow = Collections.min(mapFrom.values());
        int toHigh = Collections.max(mapFrom.values());
        Integer fromLow = Collections.min(mapTo.values());
        Integer fromHigh = Collections.max(mapTo.values());
        List<Segment> segments = new LinkedList<>();

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

    private CompletableFuture<Map<Segment, Integer>> computeEpochCutMap(Map<Long, Long> streamCut) {
        long mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        long oldest = streamCut.keySet().stream().min(Comparator.naturalOrder()).get();
        int epochLow = StreamSegmentNameUtils.getEpoch(oldest);
        int epochHigh = StreamSegmentNameUtils.getEpoch(mostRecent);

        return fetchEpochs(epochLow, epochHigh).thenApply(epochs ->  {
            List<Long> toFind = new ArrayList<>(streamCut.keySet());
            Map<Segment, Integer> resultSet = new HashMap<>();
            for (int i = epochHigh - epochLow - 1; i >= 0; i--) {
                if (toFind.isEmpty()) {
                    break;
                }
                HistoryRecord epochRecord = epochs.get(i);
                List<Long> epochSegments = epochRecord.getSegments().stream().map(Segment::segmentId).collect(Collectors.toList());
                List<Long> found = toFind.stream().filter(epochSegments::contains).collect(Collectors.toList());
                resultSet.putAll(found.stream().collect(Collectors.toMap(x -> epochRecord.getSegments().stream()
                        .filter(z -> z.segmentId() == x).findFirst().get(), x -> epochRecord.getEpoch())));

                toFind.removeAll(epochSegments);
            }
            return resultSet;
        });
    }

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
                                                                long scaleTimestamp, boolean runOnlyIfStarted, HistoryRecord currentEpoch) {
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

    private CompletableFuture<Void> verifyNotSealed() {
        return getState(false).thenApply(state -> {
            if (state.equals(State.SEALING) || state.equals(State.SEALED)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> scaleCreateNewSegments(boolean isManualScale) {
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
                    List<Segment> newSegments = epochTransition.getNewSegmentsWithRange().entrySet().stream()
                            .map(x -> new Segment(x.getKey(), epochTransition.getTime(), x.getValue().getKey(), x.getValue().getKey()))
                            .collect(Collectors.toList());
                    HistoryRecord epochRecord = getNewEpochRecord(currentEpoch, epochTransition.getNewEpoch(), epochTransition.getActiveEpoch(),
                            newSegments, epochTransition.getSegmentsToSeal(), epochTransition.getTime());

                    HistoryTimeSeriesRecord timeSeriesRecord = HistoryTimeSeriesRecord.builder().epoch(epochTransition.getNewEpoch()).referenceEpoch(epochTransition.getNewEpoch())
                            .segmentsCreated(newSegments).segmentsSealed(Collections.emptyList()).creationTime(epochTransition.getTime()).build();
                    HistoryIndexRecord indexRecord = new HistoryIndexRecord(epochRecord.getScaleTime(), epochRecord.getEpoch());
                    return createEpochRecord(epochRecord)
                            .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecord))
                            .thenCompose(x -> updateHistoryIndex(indexRecord));
                })));
    }

    private CompletableFuture<Void> updateHistoryIndex(HistoryIndexRecord indexRecord) {
        // compute history leaf
        int historyLeaf = indexRecord.getEpoch() / HISTORY_CHUNK_SIZE;
        boolean isFirst = indexRecord.getEpoch() % HISTORY_CHUNK_SIZE == 0;
        if (isFirst) {
            return getHistoryIndexRootNodeData()
                    .thenCompose(indexRootData -> {
                        HistoryIndexRootNode indexRoot = HistoryIndexRootNode.parse(indexRootData.getData());
                        HistoryIndexRootNode update = HistoryIndexRootNode.addNewLeaf(indexRoot, indexRecord.getTime());
                        return updateHistoryIndexRootData(new Data<>(update.toByteArray(), indexRootData.getVersion()));
                    })
                    .thenCompose(v -> {
                        HistoryIndexLeaf historyIndexLeaf = new HistoryIndexLeaf(Lists.newArrayList(indexRecord));
                        return createHistoryLeaf(historyLeaf, historyIndexLeaf.toByteArray());
                    });
        } else {
            return getHistoryIndexLeafData(historyLeaf)
                    .thenCompose(historyLeafData -> {
                        HistoryIndexLeaf leaf = HistoryIndexLeaf.parse(historyLeafData.getData());
                        HistoryIndexLeaf update = HistoryIndexLeaf.addRecord(leaf, indexRecord);
                        return updateHistoryLeafData(new Data<>(update.toByteArray(), historyLeafData.getVersion()));
                    });
        }

    }


    private CompletableFuture<Void> updateHistoryTimeSeries(HistoryTimeSeriesRecord record) {
        int historyChunk = record.getEpoch() / HISTORY_CHUNK_SIZE;
        boolean isFirst = record.getEpoch() % HISTORY_CHUNK_SIZE == 0;

        if (isFirst) {
            return createHistoryTimeSeriesChunk(historyChunk, record);
        } else {
            return getHistoryTimeSeriesChunkData(historyChunk)
                    .thenCompose(x -> {
                        HistoryTimeSeries historyChunkTimeSeries = HistoryTimeSeries.parse(x.getData());
                        HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(historyChunkTimeSeries, record);
                        return updateHistoryTimeSeriesChunkData(historyChunk, new Data<>(update.toByteArray(), x.getVersion()));
                    });
        }
    }

    private CompletableFuture<Void> createEpochRecord(HistoryRecord epoch) {
        return createEpochRecordData(new Data<>(epoch.toByteArray(), 0));
    }

    private CompletableFuture<EpochTransitionRecord> migrateManualScaleToNewEpoch(Data<T> epochTransitionData) {
        EpochTransitionRecord epochTransition = EpochTransitionRecord.parse(epochTransitionData.getData());
        CompletableFuture<HistoryRecord> activeEpochFuture = getCurrentEpochRecord();
        CompletableFuture<HistoryRecord> epochRecordActiveEpochFuture = getEpochRecord(epochTransition.getActiveEpoch());

        return CompletableFuture.allOf(activeEpochFuture, epochRecordActiveEpochFuture)
                .thenCompose(x -> {
                    HistoryRecord activeEpoch = activeEpochFuture.join();
                    HistoryRecord epochRecordActiveEpoch = epochRecordActiveEpochFuture.join();
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
     *
     * @return Future which when complete will have the history record updated in store.
     */
    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated() {
        // add segment sealed epoch. Once we add these record, successors can be computed on sealed segments.
        // note: we have not yet sealed the segments though as that will be next step.
        return getEpochTransitionNode().thenCompose(epochTransitionData -> {
            EpochTransitionRecord epochTransition = EpochTransitionRecord.parse(epochTransitionData.getData());
            return Futures.allOf(epochTransition.getSegmentsToSeal().stream()
                        .map(segmentToSeal -> createSealedSegmentRecord(segmentToSeal, epochTransition.getNewEpoch())).collect(Collectors.toList()));
            });
    }

    private CompletableFuture<Void> createSealedSegmentRecord(long segmentToSeal, int newEpoch) {
        byte[] array = new byte[Integer.BYTES];
        BitConverter.writeInt(array, 0, newEpoch);
        return createSealedSegmentRecordData(segmentToSeal, array);
    }

    private CompletableFuture<Void> clearMarkers(final Set<Long> segments) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                .map(this::removeColdMarker).collect(Collectors.toList())));
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     *
     * @param sealedSegmentSizes sealed segments with sizes
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes) {
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

    private CompletableFuture<Void> updateSealedSegmentSizes(Map<Long, Long> sealedSegmentSizes) {
        Map<Integer, List<Long>> shards = sealedSegmentSizes.values().stream()
                .collect(Collectors.groupingBy(x -> StreamSegmentNameUtils.getSegmentNumber(x) / SHARD_SIZE));
        return Futures.allOf(shards.entrySet().stream().map(x -> {
            int shard = x.getKey();
            List<Long> segments = x.getValue();

            return createSegmentSealedChunk(shard).thenCompose(v -> getSealedSegmentShardData(shard)
                    .thenApply(y -> {
                        SealedSegmentsMapShard mapShard = SealedSegmentsMapShard.parse(y.getData());
                        segments.forEach(z -> mapShard.addSealedSegmentSize(z, sealedSegmentSizes.get(z)));
                        return updateSealedSegmentSizesShardData(new Data<>(mapShard.toByteArray(), y.getVersion()));
                    }));
        }).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> rollingTxnNewSegmentsCreated(Map<Long, Long> sealedTxnEpochSegments, int transactionEpoch, long time) {
        // create two new epochs
        //
        return checkState(state -> state.equals(State.COMMITTING_TXN) || state.equals(State.SEALING))
                .thenCompose(x -> getActiveEpoch(true))
                .thenCompose(activeEpochRecord -> getEpochRecord(transactionEpoch)
                        .thenCompose(transactionEpochRecord -> {
                                    // TODO: shivesh
                                    HistoryRecord duplicateActiveEpoch = null;
                                    HistoryRecord duplicateTxnEpoch = null;

                                    HistoryTimeSeriesRecord timeSeriesRecordTxnEpoch = HistoryTimeSeriesRecord.builder().epoch(duplicateTxnEpoch.getEpoch())
                                            .referenceEpoch(duplicateTxnEpoch.getReferenceEpoch()).creationTime(time).build();
                                    HistoryIndexRecord indexRecordTxnEpoch = new HistoryIndexRecord(time, duplicateTxnEpoch.getEpoch());
                                    HistoryTimeSeriesRecord timeSeriesRecordActiveEpoch = HistoryTimeSeriesRecord.builder().epoch(duplicateActiveEpoch.getEpoch())
                                            .referenceEpoch(duplicateActiveEpoch.getReferenceEpoch()).creationTime(time).build();
                                    HistoryIndexRecord indexRecordActiveEpoch = new HistoryIndexRecord(time, duplicateActiveEpoch.getEpoch());
                                    return createEpochRecord(duplicateTxnEpoch)
                                            .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordTxnEpoch))
                                            .thenCompose(x -> updateHistoryIndex(indexRecordTxnEpoch))
                                            .thenCompose(x -> createEpochRecord(duplicateActiveEpoch))
                                            .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordActiveEpoch))
                                            .thenCompose(x -> updateHistoryIndex(indexRecordActiveEpoch))
                                            .thenCompose(x -> Futures.allOf(activeEpochRecord.getSegments().stream().map(segment ->
                                                    createSealedSegmentRecord(segment.segmentId(), duplicateTxnEpoch.getEpoch())).collect(Collectors.toList())))
                                            .thenCompose(x -> Futures.allOf(duplicateTxnEpoch.getSegments().stream().map(segment ->
                                                    createSealedSegmentRecord(segment.segmentId(), duplicateActiveEpoch.getEpoch())).collect(Collectors.toList())));
                                })
                .thenCompose(v -> updateSealedSegmentSizes(sealedTxnEpochSegments)));
    }

    @Override
    public CompletableFuture<Void> rollingTxnActiveEpochSealed(Map<Long, Long> sealedActiveEpochSegments, int activeEpoch) {
        return checkState(state -> state.equals(State.COMMITTING_TXN) || state.equals(State.SEALING))
                .thenCompose(v -> updateSealedSegmentSizes(sealedActiveEpochSegments))
                .thenCompose(x -> clearMarkers(sealedActiveEpochSegments.keySet()))
                .thenCompose(x -> updateCurrentEpochRecord(activeEpoch + 2));
    }

    private CompletableFuture<Void> updateCurrentEpochRecord(int activeEpoch) {
        return getEpochRecord(activeEpoch)
                .thenCompose(epochRecord -> getCurrentEpochRecordData()
                        .thenCompose(currentEpochRecordData -> updateCurrentEpochRecordData(
                            new Data<>(epochRecord.toByteArray(), currentEpochRecordData.getVersion()))));
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
            int dataVersion = version.isPresent() ? version.get() : data.getVersion();
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
    public CompletableFuture<HistoryRecord> getActiveEpoch(boolean ignoreCached) {
        return getCurrentEpochRecordData().thenApply(currentEpochRecord -> HistoryRecord.parse(currentEpochRecord.getData()));
    }

    @Override
    public CompletableFuture<HistoryRecord> getEpochRecord(int epoch) {
        return getEpochRecordData().thenApply(epochRecordData -> HistoryRecord.parse(epochRecordData.getData()));
    }

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

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut, Optional<StreamCutRecord> reference) {
        Map<Long, Long> referenceStreamCut = reference.map(streamCutRecord -> streamCutRecord.getStreamCut().entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().segmentId(), Map.Entry::getValue))).orElse(Collections.emptyMap());
        return getSegmentsBetweenStreamCuts(referenceStreamCut, streamCut)
                .thenCompose(segments -> {
                    Map<Integer, List<Segment>> shards = segments.stream().collect(Collectors.groupingBy(x -> x.getNumber() / SHARD_SIZE));
                    return Futures.allOfWithResults(shards.entrySet().stream()
                            .map(entry -> getSealedSegmentShard(entry.getKey())
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

    private CompletableFuture<SealedSegmentsMapShard> getSealedSegmentShard(Integer key) {
        return getSealedSegmentShardData(key).thenApply(x -> SealedSegmentsMapShard.parse(x.getData()));
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord streamCut) {
        return getRetentionSetData()
                .thenCompose(data -> {
                    RetentionSet retention = RetentionSet.parse(data.getData());

                    RetentionSet update = RetentionSet.addStreamCutIfLatest(retention, streamCut);
                    return createStreamCutRecord(new Data<>(update.toByteArray(), data.getVersion()))
                        .thenCompose(v -> updateRetentionSetData(new Data<>(update.toByteArray(), data.getVersion())));
                });
    }

    @Override
    public CompletableFuture<RetentionSet> getRetentionSet() {
        return getRetentionSetData()
                .thenApply(data -> RetentionSet.parse(data.getData()));
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
    public CompletableFuture<Void> deleteCommittingTransactionsRecord() {
        return deleteCommittingTxnRecord();
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(final int epoch) {
        return getTxnInEpoch(epoch)
                .thenApply(x -> x.entrySet()
                        .stream()
                        .collect(toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxnRecord.parse(v.getValue().getData()))));
    }

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

    private HistoryRecord getNewEpochRecord(final HistoryRecord lastRecord, final int epoch, final int referenceEpoch,
                                            final Collection<Segment> createdSegments, final Collection<Long> sealedSegments, final long time) {
        List<Segment> segments = lastRecord.getSegments();
        segments = segments.stream().filter(x -> sealedSegments.stream().anyMatch(y -> y == x.segmentId())).collect(Collectors.toList());
        segments.addAll(createdSegments);
        return HistoryRecord.builder().epoch(epoch).referenceEpoch(referenceEpoch).segments(segments).creationTime(time).build();
    }

    int getTransactionEpoch(UUID txId) {
        // epoch == UUID.msb >> 32
        return TableHelper.getTransactionEpoch(txId);
    }

    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration,
                                                                       final long creationTime, final int startingSegmentNumber);
    abstract CompletableFuture<Void> deleteStream();

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration);

    abstract CompletableFuture<Void> setConfigurationData(final Data<T> configuration);

    abstract CompletableFuture<Data<T>> getConfigurationData(boolean ignoreCached);

    abstract CompletableFuture<Void> setTruncationData(final Data<T> truncationRecord);

    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncationRecord);

    abstract CompletableFuture<Data<T>> getTruncationData(boolean ignoreCached);

    abstract CompletableFuture<Void> createStateIfAbsent(final State state);

    abstract CompletableFuture<Void> setStateData(final Data<T> state);

    abstract CompletableFuture<Data<T>> getStateData(boolean ignoreCached);

    abstract CompletableFuture<Void> createRetentionSetData(Data<Integer> data);

    abstract CompletableFuture<Void> createSegmentSealedChunkData(int shardNumber, byte[] data);

    abstract CompletableFuture<Void> createHistoryLeaf(int leafChunkNumber, byte[] data);

    abstract CompletableFuture<Void> createHistoryIndexRoot(byte[] data);
    abstract CompletableFuture<Void> createHistoryTimeSeriesChunkData(int chunkNumber, byte[] data);

    abstract CompletableFuture<Void> createCurrentEpochRecordData(byte[] data);
    abstract CompletableFuture<Void> updateCurrentEpochRecordData(Data<T> data);
    abstract CompletableFuture<Data<T>> getCurrentEpochRecordData();

    abstract CompletableFuture<Data<T>> getHistoryTimeSeriesChunkData(int chunkNumber);

    abstract CompletableFuture<Data<T>> getSegmentSealedRecordData(long segmentId);

    abstract CompletableFuture<Data<T>> getHistoryIndexLeafData(int leaf);
    abstract CompletableFuture<Data<T>> getHistoryIndexRootNodeData();
    abstract CompletableFuture<Void> updateHistoryIndexRootData(final Data<T> updated);

    abstract CompletableFuture<Void> updateHistoryLeafData(Data<T> tData);
    abstract CompletionStage<Void> updateHistoryTimeSeriesChunkData(int historyChunk, Data<T> tData);
    abstract CompletableFuture<Void> createEpochRecordData(Data<Integer> data);
    abstract CompletableFuture<Void> createSealedSegmentRecordData(long segmentToSeal, byte[] data);
    abstract CompletableFuture<Void> updateSealedSegmentSizesShardData(Data<T> data);

    abstract CompletableFuture<Data<T>> getSealedSegmentShardData(int shard);
    abstract CompletableFuture<Data<T>> getEpochRecordData();

    abstract CompletableFuture<Void> createStreamCutRecord(Data<T> tData);

    abstract CompletableFuture<Void> updateRetentionSetData(Data<T> tData);

    abstract CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime);

    abstract CompletableFuture<Data<T>> getRetentionSetData();

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

    abstract CompletableFuture<Void> createMarkerData(long segmentId, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(long segmentId, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(long segmentId);

    abstract CompletableFuture<Data<T>> getMarkerData(long segmentId);

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Map<String, Data<T>>> getTxnInEpoch(int epoch);

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;

    abstract CompletableFuture<Void> createEpochTransitionNode(byte[] epochTransition);

    abstract CompletableFuture<Void> updateEpochTransitionNode(Data<T> epochTransition);

    abstract CompletableFuture<Data<T>> getEpochTransitionNode();

    abstract CompletableFuture<Void> createCommittingTxnRecord(byte[] committingTxns);

    abstract CompletableFuture<Data<T>> getCommittingTxnRecord();

    abstract CompletableFuture<Void> deleteCommittingTxnRecord();

    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data);

    abstract CompletableFuture<Data<T>> getWaitingRequestNode();

    abstract CompletableFuture<Void> deleteWaitingRequestNode();
}
