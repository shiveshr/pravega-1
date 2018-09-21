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
     * Method to validate supplied scale input. It performs a check that new ranges are identical to sealed ranges.
     *
     * @param segmentsToSeal segments to seal
     * @param newRanges      new ranges to create
     * @return true if scale input is valid, false otherwise.
     */
    public static boolean isScaleInputValid(final List<Long> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final HistoryRecord currentEpoch) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segmentId -> currentEpoch.getSegments().stream().filter(x -> x.segmentId() == segmentId).findFirst().map(x ->
                        new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
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
}
