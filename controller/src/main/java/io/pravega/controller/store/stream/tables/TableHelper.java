/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Lombok;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;

/**
 * Helper class for operations pertaining to segment store tables (segment, history, index).
 * All the processing is done locally and this class does not make any network calls.
 * All methods are synchronous and blocking.
 */
public class TableHelper {
    /**
     * Get active segments at given timestamp.
     * Perform binary search on index table to find the record corresponding to timestamp.
     * Once we find the segments, compare them to truncationRecord and take the more recent of the two.
     *
     * @param truncationRecord truncation record
     * @return list of active segments.
     */
    public static Map<Segment, Long> getActiveSegments(HistoryRecord record, final StreamTruncationRecord truncationRecord) {

        Map<Segment, Long> segmentsWithOffset;
        if (truncationRecord.equals(StreamTruncationRecord.EMPTY)) {
            segmentsWithOffset = record.getSegments().stream().collect(Collectors.toMap(x -> x, x -> 0L));
        } else {
            // case 1: if record.epoch is before truncation, simply pick the truncation stream cut
            if (record.getEpoch() < truncationRecord.getTruncationEpochLow()) {
                segmentsWithOffset = truncationRecord.getStreamCut().entrySet().stream()
                        .collect(Collectors.toMap(e -> truncationRecord.getCutEpochMap().keySet().stream()
                                .filter(x -> x.segmentId() == e.getKey()).findAny().get(), Map.Entry::getValue));
            } else if (record.getEpoch() > truncationRecord.getTruncationEpochHigh()) {
                // case 2: if record.epoch is after truncation, simply use the record epoch
                segmentsWithOffset = record.getSegments().stream().collect(Collectors.toMap(x -> x,
                        x -> truncationRecord.getStreamCut().getOrDefault(x, 0L)));
            } else {
                // case 3: overlap between requested epoch and stream cut.
                // take segments from stream cut that are from or after this epoch.
                // take remaining segments from this epoch.
                segmentsWithOffset = new HashMap<>();
                // all segments from stream cut that have epoch >= this epoch
                List<Segment> fromStreamCut = truncationRecord.getCutEpochMap().entrySet().stream()
                        .filter(x -> x.getValue() >= record.getEpoch())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                // add segments from the truncation record with corresponding offsets 
                fromStreamCut.forEach(x -> segmentsWithOffset.put(x, truncationRecord.getStreamCut().get(x)));
                
                // put remaining segments as those that dont overlap with ones taken from streamCut.
                // Note: we will use the head of these segments, basically offset = 0
                record.getSegments().stream().filter(x -> fromStreamCut.stream().noneMatch(x::overlaps))
                        .forEach(x -> segmentsWithOffset.put(x, 0L));
            }
        }
        return segmentsWithOffset;
    }

    /**
     * Method to validate a given stream Cut.
     * A stream cut is valid if it covers the entire key space without any overlaps in ranges for segments that form the
     * streamcut. It throws {@link IllegalArgumentException} if the supplied stream cut does not satisfy the invariants.
     *
     * @param streamCut supplied stream cut.
     */
    public static void validateStreamCut(List<AbstractMap.SimpleEntry<Double, Double>> streamCut) {
        // verify that stream cut covers the entire range of 0.0 to 1.0 keyspace without overlaps.
        List<AbstractMap.SimpleEntry<Double, Double>> reduced = reduce(streamCut);
        Exceptions.checkArgument(reduced.size() == 1 && reduced.get(0).getKey().equals(0.0) &&
                        reduced.get(0).getValue().equals(1.0), "streamCut",
                " Invalid input, Stream Cut does not cover full key range.");
    }

    /**
     * Method to find all segments between given from and to stream cuts.
     * @param from                     stream cut to truncate at.
     * @param to                       stream cut to truncate at.
     * @return returns segments that fall between given stream cuts
     */
    public static List<Segment> findSegmentsBetweenStreamCuts(List<HistoryRecord> epochs,
                                                                 final Map<Long, Long> from,
                                                                 final Map<Long, Long> to) {
        Preconditions.checkArgument(!(from.isEmpty() && to.isEmpty()));
        // 1. compute epoch cut map for from and to
        Map<Segment, Integer> fromEpochCutMap = from.isEmpty() ? Collections.emptyMap() :
                computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, from);
        Map<Segment, Integer> toEpochCutMap = to.isEmpty() ? Collections.emptyMap() :
                computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, to);
        Preconditions.checkArgument(greaterThan(toEpochCutMap, fromEpochCutMap, to, from));

        Set<Long> segments = new HashSet<>();

        // if from is empty, lower bound becomes lowest possible epoch = 0
        final int fromLowEpoch = fromEpochCutMap.values().stream().min(Comparator.naturalOrder()).orElse(0);
        final int fromHighEpoch = fromEpochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(0);

        // if to is empty, upper bound becomes highest epoch
        final int highestEpoch = HistoryRecord.readLatestRecord(historyIndex, historyTable, true).get().getEpoch();
        final int toLowEpoch = toEpochCutMap.values().stream().min(Comparator.naturalOrder()).orElse(highestEpoch);
        final int toHighEpoch = toEpochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(highestEpoch);

        // 2. loop from.lowestEpoch till to.highestEpoch
        for (int epoch = fromLowEpoch; epoch <= toHighEpoch; epoch++) {
            HistoryRecord epochRecord = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false).get();

            // for epochs that cleanly lie between from.high and to.low epochs we can include all segments present in them
            // because they are guaranteed to be greater than `from` and less than `to` stream cuts.
            if (epoch >= fromHighEpoch && epoch <= toLowEpoch) {
                segments.addAll(epochRecord.getSegments());
            } else {
                // 3. for each segment in epoch.segments, find overlaps in from and to
                epochRecord.getSegments().stream().filter(x -> !segments.contains(x)).forEach(segmentId -> {
                    // 4. if segment.number >= from.segmentNumber && segment.number <= to.segmentNumber include segment.number
                    Segment epochSegment = getSegment(segmentId, segmentIndex, segmentTable, historyIndex, historyTable);
                    boolean greatThanFrom = fromEpochCutMap.keySet().stream().filter(x -> x.overlaps(epochSegment))
                            .allMatch(x -> x.segmentId() <= epochSegment.segmentId());
                    boolean lessThanTo = toEpochCutMap.keySet().stream().filter(x -> x.overlaps(epochSegment))
                            .allMatch(x -> epochSegment.segmentId() <= x.segmentId());
                    if (greatThanFrom && lessThanTo) {
                        segments.add(epochSegment.segmentId());
                    }
                });
            }
        }

        return segments.stream().map(segmentId -> getSegment(segmentId, segmentIndex, segmentTable, historyIndex, historyTable))
                .collect(Collectors.toList());
    }

    /**
     * A method to compute size of stream in bytes from start till given stream cut.
     * Note: this computed size is absolute size and even if the stream has been truncated, this size is computed for the
     * entire amount of data that was written into the stream.
     *
     * @param streamCut stream cut to compute size till
     * @param sealedSegmentsRecord record for all the sealed segments for the given stream.
     * @return size (in bytes) of stream till the given stream cut.
     */
    public static long getSizeTillStreamCut(final Map<Long, Long> streamCut,
                                            final SealedSegmentsMapShard sealedSegmentsRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(sealedSegmentsRecord);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());
        Map<Segment, Integer> epochCutMap = computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        AtomicLong size = new AtomicLong();
        Map<Long, Long> sealedSegmentSizeMap = sealedSegmentsRecord.getSealedSegmentsSizeMap();

        // add sizes for segments in stream cut
        streamCut.forEach((key, value) -> size.addAndGet(value));

        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(0, historyIndex, historyTable, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            size.addAndGet(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable, historyIndex, historyTable);
                return epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().segmentId() == epochSegment.segmentId() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).map(sealedSegmentSizeMap::get).reduce((x, y) -> x + y).orElse(0L));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyIndex, historyTable, true);
        }

        return size.get();
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates for overlap
     * @return overlapping segments with current segment
     */
    public static List<Long> getOverlaps(
            final Segment current,
            final List<Segment> candidates) {
        return candidates.stream().filter(x -> x.overlaps(current)).map(Segment::segmentId).collect(Collectors.toList());
    }

    /**
     * Method to check scale operation can be performed with given input.
     * @param segmentsToSeal segments to seal
\     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Long> segmentsToSeal, final HistoryRecord currentEpoch) {
        return segmentsToSeal.stream().allMatch(x -> currentEpoch.getSegments().stream().anyMatch(y -> y.segmentId() == x));
    }

    /**
     * Method that looks at the supplied epoch transition record and compares it with partial state in metadata store to determine
     * if the partial state corresponds to supplied input.
     *
     * @param epochTransitionRecord epoch transition record
     * @return true if input matches partial state, false otherwise
     */
    public static boolean isEpochTransitionConsistent(final EpochTransitionRecord epochTransitionRecord,
                                                      HistoryRecord currentEpoch) {
        // current epoch should either match the active epoch or new epoch.

        AtomicBoolean isConsistent = new AtomicBoolean(true);

        if (currentEpoch.getEpoch() == epochTransitionRecord.activeEpoch) {
            isConsistent.compareAndSet(true,
                    currentEpoch.getSegments().containsAll(epochTransitionRecord.segmentsToSeal));
        } else if (currentEpoch.getEpoch() == epochTransitionRecord.getNewEpoch()) {
            // if history table is updated
            boolean check = latestHistoryRecord.getSegments().containsAll(epochTransitionRecord.newSegmentsWithRange.keySet()) &&
                    epochTransitionRecord.segmentsToSeal.stream().noneMatch(x -> latestHistoryRecord.getSegments().contains(x));

            isConsistent.compareAndSet(true, check);
        } else {
            isConsistent.set(false);
        }

        return isConsistent.get();
    }

    /**
     * Return the active epoch. The active epoch is the one whose segments are not sealed yet.
     * It is typically the latest epoch. However, if the latest epoch is "partial" then it is one of the previous epochs.
     * During scale we only add one new epoch at a time, so active epoch becomes epoch that is previous to partial epoch.
     * However, during commit, it will always be behind by two.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static HistoryRecord getActiveEpoch(final byte[] historyIndex, final byte[] historyTable) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        // if the record is created as a sealed epoch, the previous epoch may not have been sealed yet and hence that will be active.
        if (historyRecord.isPartial()) {
            // Scale creates new fresh epochs. So if latest epoch is partial and a duplicate, then its previous epoch
            // is created by rolling txn workflow.
            // So for partial epochs created by scale, go back one epoch from latest to get active epoch. For rolling txn
            // go back by two epochs.
            if (historyRecord.isDuplicate()) {
                historyRecord = HistoryRecord.readRecord(historyRecord.getEpoch() - 2, historyIndex, historyTable, true).get();
            } else {
                historyRecord = HistoryRecord.fetchPrevious(historyRecord, historyIndex, historyTable).get();
            }
        }

        return historyRecord;
    }

    /**
     * Return segments in the epoch.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return segments in the epoch
     */
    public static List<Long> getSegmentsInEpoch(final byte[] historyIndex, final byte[] historyTable, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table")).getSegments();
    }

    /**
     * Method to fetch history record corresponding to the given epoch.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return History record corresponding to the epoch
     */
    public static HistoryRecord getEpochRecord(final byte[] historyIndex, final byte[] historyTable, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table"));
    }

    private static List<Long> diff(List<Long> list1, List<Long> list2) {
        return list1.stream().filter(z -> !list2.contains(z)).collect(Collectors.toList());
    }

    private static HistoryRecord findRecordInHistoryTable(long timestamp, byte[] historyIndex, byte[] historyTable) {
        int latestEpoch = HistoryIndexRecord.readLatestRecord(historyIndex).get().getEpoch();

        final Optional<HistoryRecord> historyRecord = binarySearchHistory(0, latestEpoch, timestamp, historyIndex,
                historyTable);
        return historyRecord.orElseGet(() -> HistoryRecord.readRecord(0, historyIndex, historyTable, true).get());
    }

    private static Optional<HistoryRecord> binarySearchHistory(final int lowerEpoch,
                                                   final int upperEpoch,
                                                   final long timestamp,
                                                   final byte[] historyIndex,
                                                   final byte[] historyTable) {
        if (upperEpoch < lowerEpoch) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(middle, historyIndex, historyTable, false);

        if (record.get().getScaleTime() <= timestamp) {
            Optional<HistoryRecord> next = HistoryRecord.fetchNext(record.get(), historyIndex, historyTable, false);
            if (!next.isPresent() || (next.get().getScaleTime() > timestamp)) {
                return record;
            } else {
                return binarySearchHistory(middle + 1, upperEpoch, timestamp,
                        historyIndex, historyTable);
            }
        } else {
            return binarySearchHistory(lowerEpoch, middle - 1, timestamp, historyIndex, historyTable);
        }
    }

    /**
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or
     * equal to segment creation epoch
     * @param lowerEpoch starting record number in index table from where to search
     * @param upperEpoch last record number in index table till where to search
     * @param segmentId segment number to find sealed event
     * @param historyIndex index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lowerEpoch,
                                                                  final int upperEpoch,
                                                                  final long segmentId,
                                                                  final byte[] historyIndex,
                                                                  final byte[] historyTable) {
        if (lowerEpoch > upperEpoch || historyTable.length == 0) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(middle, historyIndex, historyTable, false);
        assert record.isPresent();
        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentId)) {
            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(middle - 1, historyIndex, historyTable,
                    false);
            assert previousRecord.isPresent();
            if (previousRecord.get().getSegments().contains(segmentId)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lowerEpoch,
                        (lowerEpoch + upperEpoch) / 2 - 1,
                        segmentId,
                        historyIndex,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lowerEpoch + upperEpoch) / 2 + 1,
                    upperEpoch,
                    segmentId,
                    historyIndex,
                    historyTable);
        }
    }

    /**
     * Method to validate supplied scale input. It performs a check that new ranges are identical to sealed ranges.
     *
     * @param segmentsToSeal segments to seal
     * @param newRanges      new ranges to create
     * @param segmentTable   segment table
     * @param segmentIndex   segment index
     * @return true if scale input is valid, false otherwise.
     */
    public static boolean isScaleInputValid(final List<Long> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final HistoryRecord currentEpoch) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segmentId -> SegmentRecord.readRecord(segmentIndex, segmentTable, getSegmentNumber(segmentId)).map(x ->
                        new AbstractMap.SimpleEntry<>(x.getRoutingKeyStart(), x.getRoutingKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    /**
     * Method to transform epoch cut map with segment id to epoch cut map with Segment object.
     */
    private static Map<Segment, Integer> transform(byte[] segmentIndex, byte[] segmentTable, byte[] historyIndex, byte[] historyTable, Map<Long, Integer> epochStreamCutMap) {
        return epochStreamCutMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> getSegment(entry.getKey(), segmentIndex, segmentTable, historyIndex, historyTable),
                        Map.Entry::getValue));
    }

    /**
     * Method to compute epoch transition record. It takes segments to seal and new ranges and all the tables and
     * computes the next epoch transition record.
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges
     * @param scaleTimestamp scale time
     * @return new epoch transition record based on supplied input
     */
    public static EpochTransitionRecord computeEpochTransition(HistoryRecord currentEpoch, List<Long> segmentsToSeal,
                                                         List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        Preconditions.checkState(currentEpoch.getSegments().containsAll(segmentsToSeal), "Invalid epoch transition request");

        int newEpoch = currentEpoch.getEpoch() + 1;
        int nextSegmentNumber = currentEpoch.getSegments().stream().mapToInt(Segment::getNumber).max().getAsInt();
        Map<Long, AbstractMap.SimpleEntry<Double, Double>> newSegments = new HashMap<>();
        IntStream.range(0, newRanges.size()).forEach(x -> {
            newSegments.put(computeSegmentId(nextSegmentNumber + x, newEpoch), newRanges.get(x));
        });
        return new EpochTransitionRecord(currentEpoch.getEpoch(), scaleTimestamp, ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegments));

    }

    public static boolean verifyRecordMatchesInput(List<Long> segmentsToSeal, List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                             boolean isManualScale, EpochTransitionRecord record) {
        boolean newRangeMatch = newRanges.stream().allMatch(x ->
                record.getNewSegmentsWithRange().values().stream()
                        .anyMatch(y -> y.getKey().equals(x.getKey())
                                && y.getValue().equals(x.getValue())));
        boolean segmentsToSealMatch = record.getSegmentsToSeal().stream().allMatch(segmentsToSeal::contains) ||
                (isManualScale && record.getSegmentsToSeal().stream().map(StreamSegmentNameUtils::getSegmentNumber).collect(Collectors.toSet())
                        .equals(segmentsToSeal.stream().map(StreamSegmentNameUtils::getSegmentNumber).collect(Collectors.toSet())));

        return newRangeMatch && segmentsToSealMatch;
    }

    /**
     * If a stream cut spans across multiple epochs then this map captures mapping of segments from the stream cut to
     * epochs they were found in closest to truncation point.
     * This data structure is used to find active segments wrt a stream cut.
     * So for example:
     * epoch 0: 0, 1
     * epoch 1: 0, 2, 3
     * epoch 2: 0, 2, 4, 5
     * epoch 3: 0, 4, 5, 6, 7
     *
     * Following is a valid stream cut {0/offset, 3/offset, 6/offset, 7/offset}
     * This spans from epoch 1 till epoch 3. Any request for segments at epoch 1 or 2 or 3 will need to have this stream cut
     * applied on it to find segments that are available for consumption.
     *
     * This method takes a stream cut and maps it to highest epochs per segment in the stream cut.
     * So in the above example, the map produced for {0/offset, 3/offset, 6/offset, 7/offset} will be
     * {0/3, 3/1, 6/3, 7/3}
     */
    private static Map<Long, Integer> computeEpochCutMap(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                                              byte[] segmentTable, Map<Long, Long> streamCut) {
        Map<Long, Integer> epochStreamCutMap = new HashMap<>();

        Long mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        Segment mostRecentSegment = getSegment(mostRecent, segmentIndex, segmentTable, historyIndex, historyTable);

        final Optional<HistoryRecord> highEpochRecord = HistoryRecord.readRecord(mostRecentSegment.getEpoch(), historyIndex,
                historyTable, false);

        List<Long> toFind = new ArrayList<>(streamCut.keySet());
        Optional<HistoryRecord> epochRecord = highEpochRecord;

        while (epochRecord.isPresent() && !toFind.isEmpty()) {
            List<Long> epochSegments = epochRecord.get().getSegments();
            Map<Boolean, List<Long>> group = toFind.stream().collect(Collectors.groupingBy(x -> epochSegments.contains(x)));
            toFind = Optional.ofNullable(group.get(false)).orElse(Collections.emptyList());
            int epoch = epochRecord.get().getEpoch();
            List<Long> found = Optional.ofNullable(group.get(true)).orElse(Collections.emptyList());
            found.forEach(x -> epochStreamCutMap.put(x, epoch));
            epochRecord = HistoryRecord.fetchPrevious(epochRecord.get(), historyIndex, historyTable);
        }

        return epochStreamCutMap;
    }

    private static Set<Long> computeToDelete(Map<Segment, Integer> epochCutMap, byte[] historyIndex, byte[] historyTable,
                                                                  byte[] segmentIndex, byte[] segmentTable, Set<Long> deletedSegments) {
        Set<Long> toDelete = new HashSet<>();
        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);

        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(0, historyIndex, historyTable, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            toDelete.addAll(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable, historyIndex, historyTable);
                // ignore already deleted segments from todelete
                // toDelete.add(epoch.segment overlaps cut.segment && epoch < cut.segment.epoch)
                return !deletedSegments.contains(epochSegmentNumber) &&
                        epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().segmentId() == epochSegment.segmentId() ||
                                (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).collect(Collectors.toSet()));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyIndex, historyTable, true);
        }
        return toDelete;
    }

    private static boolean greaterThan(Map<Segment, Integer> map1, Map<Segment, Integer> map2, Map<Long, Long> cut1,
                                       Map<Long, Long> cut2) {
        // find overlapping segments in map2 for all segments in map1
        // compare epochs. map1 should have epochs gt or eq its overlapping segments in map2
        return map1.entrySet().stream().allMatch(e1 ->
                map2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().segmentId() == e1.getKey().segmentId() &&
                                cut1.get(e1.getKey().segmentId()) < cut2.get(e2.getKey().segmentId()))
                                || (e2.getKey().overlaps(e1.getKey()) && e1.getValue() < e2.getValue())));
    }

    /**
     * Helper method to compute list of continuous ranges. For example, two neighbouring key ranges where,
     * range1.high == range2.low then they are considered neighbours.
     * This method reduces input range into distinct continuous blocks.
     * @param input list of key ranges.
     * @return reduced list of key ranges.
     */
    private static List<AbstractMap.SimpleEntry<Double, Double>> reduce(List<AbstractMap.SimpleEntry<Double, Double>> input) {
        List<AbstractMap.SimpleEntry<Double, Double>> ranges = new ArrayList<>(input);
        ranges.sort(Comparator.comparingDouble(AbstractMap.SimpleEntry::getKey));
        List<AbstractMap.SimpleEntry<Double, Double>> result = new ArrayList<>();
        double low = -1.0;
        double high = -1.0;
        for (AbstractMap.SimpleEntry<Double, Double> range : ranges) {
            if (high < range.getKey()) {
                // add previous result and start a new result if prev.high is less than next.low
                if (low != -1.0 && high != -1.0) {
                    result.add(new AbstractMap.SimpleEntry<>(low, high));
                }
                low = range.getKey();
                high = range.getValue();
            } else if (high == range.getKey()) {
                // if adjacent (prev.high == next.low) then update only high
                high = range.getValue();
            } else {
                // if prev.high > next.low.
                // [Note: next.low cannot be less than 0] which means prev.high > 0
                assert low >= 0;
                assert high > 0;
                result.add(new AbstractMap.SimpleEntry<>(low, high));
                low = range.getKey();
                high = range.getValue();
            }
        }
        // add the last range
        if (low != -1.0 && high != -1.0) {
            result.add(new AbstractMap.SimpleEntry<>(low, high));
        }
        return result;
    }

    public static long getEpochScaleTime(int epoch, byte[] historyIndex, byte[] historyTable) {
        return HistoryRecord.readRecord(epoch, historyIndex, historyTable, true)
                .map(HistoryRecord::getScaleTime).orElse(0L);
    }

    public static int getTransactionEpoch(UUID txId) {
        // epoch == UUID.msb >> 32
        return (int) (txId.getMostSignificantBits() >> 32);
    }

    /**
     * This method takes a segment id and replaces its epoch with the epoch in the transaction.
     *
     * @param segmentId segment id
     * @param txId transaction id
     * @return new segment id which uses transaction's epoch.
     */
    public static long generalizedSegmentId(long segmentId, UUID txId) {
        return computeSegmentId(getSegmentNumber(segmentId), getTransactionEpoch(txId));
    }

    /**
     * This method provides the starting segment number for this stream that is stored in the first position of the
     * segment index.
     *
     * @param segmentIndex segment index
     * @return starting segment number for this stream.
     */
    public static int getStartingSegmentNumber(byte[] segmentIndex) {
        return SegmentIndexRecord.getStartingSegmentNumberFromIndex(segmentIndex);
    }
}
