/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import lombok.Lombok;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class for operations pertaining to segment store tables (segment, history, index).
 * All the processing is done locally and this class does not make any network calls.
 * All methods are synchronous and blocking.
 */
public class TableHelper {
    /**
     * Segment Table records are indexed and and it is O(constant) operation to get segment offset given segmentIndex.
     *
     * @param number       segment number
     * @param segmentTable segment table
     * @param segmentIndex segment table index
     * @return Segment
     */
    public static Segment getSegment(final int number, final byte[] segmentTable, final byte[] segmentIndex) {

        Optional<SegmentRecord> recordOpt = SegmentRecord.readRecord(segmentTable, segmentIndex, number);
        if (recordOpt.isPresent()) {
            SegmentRecord record = recordOpt.get();
            return new Segment(record.getSegmentNumber(),
                    record.getStartTime(),
                    record.getCreationEpoch(),
                    record.getRoutingKeyStart(),
                    record.getRoutingKeyEnd());
        } else {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                    "Segment number: " + String.valueOf(number));
        }
    }

    /**
     * Helper method to get highest segment number.
     * @param segmentIndex segment table.
     * @return
     */
    public static int getLastSegmentNumber(final byte[] segmentIndex) {
        return SegmentIndexRecord.readLatestRecord(segmentIndex).get().getSegmentNumber();
    }

    /**
     * This method reads segment table and returns total number of segments in the table.
     *
     * @param segmentIndex history table.
     * @return total number of segments in the stream.
     */
    public static int getSegmentCount(final byte[] segmentIndex) {
        return SegmentIndexRecord.readLatestRecord(segmentIndex).get().getSegmentNumber();
    }

    /**
     * Current active segments correspond to last entry in the history table.
     * Until segment number is written to the history table it is not exposed to outside world
     * (e.g. callers - producers and consumers)
     *
     * @param historyTable history table
     * @return list of active segment numbers in current active epoch. This ignores partial epochs if scale operation
     * is ongoing and returns the latest completed epoch.
     */
    public static List<Integer> getActiveSegments(final byte[] historyTable, final byte[] historyIndex) {
        final Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable, historyIndex,true);

        return record.isPresent() ? record.get().getSegments() : new ArrayList<>();
    }

    /**
     * Get active segments at given timestamp.
     * Perform binary search on index table to find the record corresponding to timestamp.
     * Once we find the segments, compare them to truncationRecord and take the more recent of the two.
     * @param timestamp        timestamp
     * @param historyIndex       index table
     * @param historyTable     history table
     * @param segmentTable     segment table
     * @param truncationRecord truncation record
     * @return list of active segments.
     */
    public static List<Integer> getActiveSegments(final long timestamp, final byte[] historyIndex, final byte[] historyTable,
                                                  final byte[] segmentIndex, final byte[] segmentTable,
                                                  final StreamTruncationRecord truncationRecord) {
        Optional<HistoryRecord> recordOpt = HistoryRecord.readRecord(historyTable, historyIndex,0, true);
        if (recordOpt.isPresent() && timestamp > recordOpt.get().getScaleTime()) {
            final Optional<HistoryIndexRecord> indexOpt = HistoryIndexRecord.search(timestamp, historyIndex);
            final int startingOffset = indexOpt.map(HistoryIndexRecord::getHistoryOffset).orElse(0);

            // read the record corresponding to indexed record
            recordOpt = HistoryRecord.readRecord(historyTable, historyIndex, indexOpt.get().getEpoch(), true);
        }

        return recordOpt.map(record -> {
            List<Integer> segments;
            if (truncationRecord == null) {
                segments = record.getSegments();
            } else {
                // case 1: if record.epoch is before truncation, simply pick the truncation stream cut
                if (record.getEpoch() < truncationRecord.getTruncationEpochLow()) {
                    segments = Lists.newArrayList(truncationRecord.getStreamCut().keySet());
                } else if (record.getEpoch() > truncationRecord.getTruncationEpochHigh()) {
                    // case 2: if record.epoch is after truncation, simply use the record epoch
                    segments = record.getSegments();
                } else {
                    // case 3: overlap between requested epoch and stream cut.
                    // take segments from stream cut that are from or aftergit re this epoch.
                    // take remaining segments from this epoch.
                    segments = new ArrayList<>();
                    // all segments from stream cut that have epoch >= this epoch
                    List<Integer> fromStreamCut = truncationRecord.getCutEpochMap().entrySet().stream()
                            .filter(x -> x.getValue() >= record.getEpoch())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    segments.addAll(fromStreamCut);
                    // put remaining segments as those that dont overlap with ones taken from streamCut.
                    segments.addAll(record.getSegments().stream().filter(x -> fromStreamCut.stream().noneMatch(y ->
                            getSegment(x, segmentTable, segmentIndex).overlaps(getSegment(y, segmentTable, segmentIndex))))
                            .collect(Collectors.toList()));
                }
            }
            return segments;
        }).orElse(Collections.emptyList());
    }

    public static void validateStreamCut(List<AbstractMap.SimpleEntry<Double, Double>> list) {
        // verify that stream cut covers the entire range of 0.0 to 1.0 keyspace without overlaps.
        List<AbstractMap.SimpleEntry<Double, Double>> reduced = reduce(list);
        Exceptions.checkArgument(reduced.size() == 1 && reduced.get(0).getKey().equals(0.0) &&
                        reduced.get(0).getValue().equals(1.0), "streamCut",
                " Invalid input, Stream Cut does not cover full key range.");
    }

    public static StreamTruncationRecord computeTruncationRecord(final byte[] historyIndex, final byte[] historyTable,
                                                                 final byte[] segmentIndex, final byte[] segmentTable,
                                                                 final Map<Integer, Long> streamCut,
                                                                 final StreamTruncationRecord previousTruncationRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());

        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyTable, historyIndex, segmentTable, segmentIndex, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentTable, segmentIndex, epochCutMap);

        Map<Segment, Integer> previousCutMapSegment = transform(segmentTable, segmentIndex,
                previousTruncationRecord.getCutEpochMap());

        Exceptions.checkArgument(greaterThan(cutMapSegments, previousCutMapSegment, streamCut,
                previousTruncationRecord.getStreamCut()),
                "streamCut", "stream cut has to be strictly ahead of previous stream cut");

        Set<Integer> toDelete = computeToDelete(cutMapSegments, historyTable, historyIndex, segmentTable, segmentIndex,
                previousTruncationRecord.getDeletedSegments());
        return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                previousTruncationRecord.getDeletedSegments(), ImmutableSet.copyOf(toDelete), true);
    }

    /**
     * A method to compute size of stream in bytes from start till given stream cut.
     * Note: this computed size is absolute size and even if the stream has been truncated, this size is computed for the
     * entire amount of data that was written into the stream.
     *
     * @param historyIndex history index for the stream
     * @param historyTable history table for the stream
     * @param segmentIndex segment index for the stream
     * @param segmentTable segment table for the stream
     * @param streamCut stream cut to compute size till
     * @param sealedSegmentsRecord record for all the sealed segments for the given stream.
     * @return size (in bytes) of stream till the given stream cut.
     */
    public static long getSizeTillStreamCut(final byte[] historyIndex, final byte[] historyTable, final byte[] segmentIndex,
                                            final byte[] segmentTable, final Map<Integer, Long> streamCut,
                                            final SealedSegmentsRecord sealedSegmentsRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(sealedSegmentsRecord);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());
        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyTable, historyIndex, segmentTable, segmentIndex, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentTable, segmentIndex, epochCutMap);
        AtomicLong size = new AtomicLong();
        Map<Integer, Long> sealedSegmentSizeMap = sealedSegmentsRecord.getSealedSegmentsSizeMap();

        // add sizes for segments in stream cut
        streamCut.forEach((key, value) -> size.addAndGet(value));

        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyTable, historyIndex, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            size.addAndGet(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentTable, segmentIndex);
                return cutMapSegments.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).map(sealedSegmentSizeMap::get).reduce((x, y) -> x + y).orElse(0L));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyTable, historyIndex, true);
        }

        return size.get();
    }

    public static List<Pair<Long, List<Integer>>> getScaleMetadata(byte[] historyTable, byte[] historyIndex) {
        return HistoryRecord.readAllRecords(historyTable, historyIndex);
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates
     * @return
     */
    public static List<Integer> getOverlaps(
            final Segment current,
            final List<Segment> candidates) {
        return candidates.stream().filter(x -> x.overlaps(current)).map(x -> x.getNumber()).collect(Collectors.toList());
    }

    /**
     * Find history record from the time when the given segment was sealed.
     * If segment is never sealed this method returns an empty list.
     * If segment is yet to be created, this method still returns empty list.
     *
     * Find index that corresponds to segment start event.
     * Perform binary search on index+history records to find segment seal event.
     *
     * If index table is not up to date we may have two cases:
     * 1. Segment create time > highest event time in index
     * 2. Segment seal time > highest event time in index
     *
     * For 1 we cant have any searches in index and will need to fall through
     * History table starting from last indexed record.
     *
     * For 2, fall through History Table starting from last indexed record
     * to find segment sealed event in history table.
     *
     * @param segment      segment
     * @param historyIndex   index table
     * @param historyTable history table
     * @return
     */
    public static List<Integer> findSegmentSuccessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {

        // find segment creation epoch.
        // find latest epoch from history table.
        final int creationEpoch = segment.getEpoch();
        final Optional<HistoryRecord> creationRecordOpt = HistoryRecord.readRecord(historyTable,
                historyIndex, creationEpoch, true);

        // segment information not in history table
        if (!creationRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        // take index of segment created event instead of searched index based on segment.startTime.
        final int lower = creationEpoch;

        final int upper = HistoryIndexRecord.readLatestRecord(historyIndex).map(HistoryIndexRecord::getEpoch).orElse(0);
        final HistoryRecord latest = HistoryRecord.readLatestRecord(historyTable, historyIndex, false).get();

        if (latest.getSegments().contains(segment.getNumber())) {
            // Segment is not sealed yet so there cannot be a successor.
            return new ArrayList<>();
        } else {
            // segment is definitely sealed, we should be able to find it by doing binary search to find the respective epoch.
            return findSegmentSealedEvent(
                    lower,
                    upper,
                    segment.getNumber(),
                    historyIndex,
                    historyTable).map(HistoryRecord::getSegments).get();
        }
    }

    /**
     * Method to find candidates for predecessors.
     * If segment was created at the time of creation of stream (= no predecessors)
     * it returns an empty list.
     *
     * First find the segment start time entry in the history table by using a binary
     * search on index followed by fall through History table if index is not up to date.
     *
     * Fetch the record in history table that immediately preceeds segment created entry.
     *
     * @param segment      segment
     * @param historyIndex   index table
     * @param historyTable history table
     * @return
     */
    public static List<Integer> findSegmentPredecessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyTable, historyIndex, segment.getEpoch(), false);
        if (!historyRecordOpt.isPresent()) {
            // cant compute predecessors because the segment creation entry is not present in history table yet.
            return new ArrayList<>();
        }

        final HistoryRecord record = historyRecordOpt.get();

        final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(record, historyTable, historyIndex);

        if (!previous.isPresent()) {
            return new ArrayList<>();
        } else {
            assert !previous.get().getSegments().contains(segment.getNumber());
            return previous.get().getSegments();
        }
    }

    /**
     * Add new segments to the segment table.
     * This method is designed to work with chunked creation. So it takes a
     * toCreate count and newRanges and it picks toCreate entries from the end of newranges.
     *
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return pair of serialized segment index and segment table.
     */
    public static Pair<byte[], byte[]> createSegmentTable(
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream segmentIndex = new ByteArrayOutputStream();
        writeToSegmentTableAndIndex(0, 0, newRanges, timeStamp, segmentStream, segmentIndex);

        return new ImmutablePair<>(segmentStream.toByteArray(), segmentIndex.toByteArray());
    }

    /**
     * Add new segments to the segment table.
     * This method is designed to work with chunked creation. So it takes a
     * toCreate count and newRanges and it picks toCreate entries from the end of newranges.
     *
     * @param startingSegmentNumber starting segment number
     * @param epoch                 epoch in which segment is created
     * @param segmentTable          segment table
     * @param segmentIndex          segment index
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return pair of serialized segment index and segment table.
     */
    public static Pair<byte[], byte[]> updateSegmentTable(final int startingSegmentNumber,
                                            final int epoch,
                                            final byte[] segmentTable,
                                            final byte[] segmentIndex,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {
        // TODO: shivesh idempotent for segment index

        // if segment index has the entry then segmentTable will definitely have the entry.
        // first verify if startingSegmentNumber is present in segmentIndex
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        writeToSegmentTableAndIndex(0, 0, newRanges, timeStamp, segmentStream, indexStream);
        return new ImmutablePair<>(segmentStream.toByteArray(), indexStream.toByteArray());
    }

    private static void writeToSegmentTableAndIndex(int startingSegmentNumber, int epoch,
                                                    List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                    long timeStamp,
                                                    ByteArrayOutputStream segmentStream,
                                                    ByteArrayOutputStream indexStream) {
        try {
            IntStream.range(0, newRanges.size())
                    .forEach(
                            x -> {
                                try {
                                    int offset = segmentStream.size();
                                    segmentStream.write(new SegmentRecord(startingSegmentNumber + x,
                                            timeStamp, epoch, newRanges.get(x).getKey(), newRanges.get(x).getValue())
                                            .toByteArray());
                                    indexStream.write(new SegmentIndexRecord(startingSegmentNumber + x,
                                            offset).toByteArray());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a new row to the history table. This row is only partial as it only contains list of segments.
     * Timestamp is added using completeHistoryRecord method.
     *
     * @param historyTable      history table
     * @param historyIndex      history index
     * @param newActiveSegments new active segments
     * @return
     */
    public static byte[] addPartialRecordToHistoryTable(final byte[] historyTable, final byte[] historyIndex,
                                                        final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        Optional<HistoryRecord> last = HistoryRecord.readLatestRecord(historyTable, historyIndex, false);
        assert last.isPresent() && !(last.get().isPartial());

        try {
            historyStream.write(historyTable);
            historyStream.write(HistoryRecord.builder().epoch(last.get().getEpoch() + 1)
                    .segments(newActiveSegments).build().toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Adds timestamp to the last record in the history table.
     *
     * @param historyTable         history table
     * @param historyIndex         history index
     * @param partialHistoryRecord partial history record
     * @param timestamp            scale timestamp
     * @return
     */
    public static byte[] completePartialRecordInHistoryTable(final byte[] historyTable, final byte[] historyIndex,
                                                             final HistoryRecord partialHistoryRecord,
                                                             final long timestamp) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable, historyIndex, false);
        assert record.isPresent() && record.get().isPartial() && record.get().getEpoch() == partialHistoryRecord.getEpoch();

        HistoryRecord previous = HistoryRecord.fetchPrevious(record.get(), historyTable, historyIndex).get();
        HistoryIndexRecord indexRecord = HistoryIndexRecord.readLatestRecord(historyIndex).get();
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(historyTable, 0, indexRecord.getHistoryOffset());
            historyStream.write(new HistoryRecord(partialHistoryRecord.getEpoch(), partialHistoryRecord.getSegments(),
                    timestamp).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to the history table.
     *
     * @param timestamp         timestamp
     * @param newActiveSegments new active segments
     * @return
     */
    public static byte[] createHistoryTable(final long timestamp,
                                            final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(new HistoryRecord(0, newActiveSegments, timestamp).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param timestamp     timestamp
     * @param historyOffset history Offset
     * @return
     */
    public static byte[] createHistoryIndex(final long timestamp, final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        try {
            indexStream.write(new HistoryIndexRecord(0, timestamp, historyOffset).toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return indexStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param historyIndex    index table
     * @param timestamp     timestamp
     * @param historyOffset history table offset
     * @return
     */
    public static byte[] updateHistoryIndex(final byte[] historyIndex,
                                            final long timestamp,
                                            final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        int epoch = HistoryIndexRecord.readLatestRecord(historyIndex).get().getEpoch();
        try {
            indexStream.write(historyIndex);
            indexStream.write(new HistoryIndexRecord(epoch, timestamp, historyOffset).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
        return indexStream.toByteArray();
    }

    /**
     * Method to check if a scale operation is currently ongoing.
     * @param historyTable history table
     * @param historyIndex history index
     * @param segmentTable segment table
     * @return true if a scale operation is ongoing, false otherwise
     */
    public static boolean isScaleOngoing(final byte[] historyTable, final byte[] historyIndex, final byte[] segmentTable) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, historyIndex,false).get();
        return latestHistoryRecord.isPartial() || !latestHistoryRecord.getSegments().contains(getLastSegmentNumber(segmentTable));
    }

    /**
     * Method to check if a scale operation is currently ongoing and has created a new epoch (presence of partial record).
     * @param historyTable history table
     * @param historyIndex history index
     * @return true if a scale operation is ongoing, false otherwise
     */
    public static boolean isNewEpochCreated(final byte[] historyTable, final byte[] historyIndex) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, historyIndex,false).get();
        return latestHistoryRecord.isPartial();
    }

    /**
     * Method to check if no scale operation is currently ongoing and scale operation can be performed with given input.
     * @param segmentsToSeal segments to seal
     * @param historyTable history table
     * @param historyIndex history index
     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Integer> segmentsToSeal, final byte[] historyTable, final byte[] historyIndex) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, historyIndex,false).get();
        return latestHistoryRecord.getSegments().containsAll(segmentsToSeal);
    }

    /**
     * Method that looks at the supplied input and compares it with partial state in metadata store to determine
     * if the partial state corresponds to supplied input.
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges to create
     * @param historyTable history table
     * @param historyIndex history index
     * @param segmentTable segment table
     * @param segmentIndex segment index
     * @return true if input matches partial state, false otherwise
     */
    public static boolean isRerunOf(final List<Integer> segmentsToSeal,
                    final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                    final byte[] historyTable,
                    final byte[] historyIndex,
                    final byte[] segmentTable,
                    final byte[] segmentIndex) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, historyIndex,false).get();

        int n = newRanges.size();
        List<SegmentRecord> lastN = SegmentRecord.readLastN(segmentTable, segmentIndex, n);

        boolean newSegmentsPredicate = newRanges.stream()
                .allMatch(x -> lastN.stream().anyMatch(y -> y.getRoutingKeyStart() == x.getKey() && y.getRoutingKeyEnd() == x.getValue()));
        boolean segmentToSealPredicate;
        boolean exactMatchPredicate;

        // CASE 1: only segment table is updated.. history table isnt...
        if (!latestHistoryRecord.isPartial()) {
            // it is implicit: history.latest.containsNone(lastN)
            segmentToSealPredicate = latestHistoryRecord.getSegments().containsAll(segmentsToSeal);
            assert !latestHistoryRecord.getSegments().isEmpty();
            exactMatchPredicate = latestHistoryRecord.getSegments().stream()
                    .max(Comparator.naturalOrder()).get() + n == getLastSegmentNumber(segmentTable);
        } else { // CASE 2: segment table updated.. history table updated (partial record)..
            // since latest is partial so previous has to exist
            HistoryRecord previousHistoryRecord = HistoryRecord.fetchPrevious(latestHistoryRecord, historyTable, historyIndex).get();

            segmentToSealPredicate = latestHistoryRecord.getSegments().containsAll(lastN.stream()
                    .map(SegmentRecord::getSegmentNumber).collect(Collectors.toList())) &&
                    previousHistoryRecord.getSegments().containsAll(segmentsToSeal);
            exactMatchPredicate = previousHistoryRecord.getSegments().stream()
                    .max(Comparator.naturalOrder()).get() + n == getLastSegmentNumber(segmentTable);
        }

        return newSegmentsPredicate && segmentToSealPredicate && exactMatchPredicate;
    }

    /**
     * Return the active epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static Pair<Integer, List<Integer>> getActiveEpoch(final byte[] historyTable, final byte[] historyIndex) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyTable, historyIndex, true).get();
        return new ImmutablePair<>(historyRecord.getEpoch(), historyRecord.getSegments());
    }

    /**
     * Return segments in the epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return segments in the epoch
     */
    public static List<Integer> getSegmentsInEpoch(final byte[] historyTable, final byte[] historyIndex, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(historyTable, historyIndex, epoch,false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table")).getSegments();
    }

    /**
     * Return the active epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static HistoryRecord getLatestEpoch(byte[] historyTable, byte[] historyIndex) {
        return HistoryRecord.readLatestRecord(historyTable, historyIndex, false).get();
    }

    /**
     * Method to compute segments created and deleted in latest scale event.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @return pair of segments sealed and segments created in last scale event.
     */
    public static Pair<List<Integer>, List<Integer>> getLatestScaleData(final byte[] historyTable, final byte[] historyIndex) {
        final Optional<HistoryRecord> current = HistoryRecord.readLatestRecord(historyTable, historyIndex, false);
        ImmutablePair<List<Integer>, List<Integer>> result;
        if (current.isPresent()) {
            final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(current.get(), historyTable, historyIndex);
            result = previous.map(historyRecord ->
                    new ImmutablePair<>(diff(historyRecord.getSegments(), current.get().getSegments()),
                        diff(current.get().getSegments(), historyRecord.getSegments())))
                    .orElseGet(() -> new ImmutablePair<>(Collections.emptyList(), current.get().getSegments()));
        } else {
            result = new ImmutablePair<>(Collections.emptyList(), Collections.emptyList());
        }
        return result;
    }

    private static List<Integer> diff(List<Integer> list1, List<Integer> list2) {
        return list1.stream().filter(z -> !list2.contains(z)).collect(Collectors.toList());
    }

    /**
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or equal to segmentCreatedEvent
     * @param lower starting record number in index table from where to search
     * @param upper last record number in index table till where to search
     * @param segmentNumber segment number to find sealed event
     * @param historyIndex index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lower,
                                                                  final int upper,
                                                                  final int segmentNumber,
                                                                  final byte[] historyIndex,
                                                                  final byte[] historyTable) {
        assert HistoryRecord.readRecord(historyTable, historyIndex, lower, false).get().getSegments()
                .contains(segmentNumber);
        if (lower > upper || historyTable.length == 0) {
            return Optional.empty();
        }

        final int middle = ((lower + upper) / 2);

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(historyTable, historyIndex, middle, false);

        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentNumber)) {

            // since `lower` epoch has segment and middle epoch doesnt have segment we know a previous has to exist.
            assert middle - 1 >= lower;
            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(historyTable, historyIndex,
                    middle - 1, false);
            if (previousRecord.get().getSegments().contains(segmentNumber)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lower,
                        (lower + upper) / 2 - 1,
                        segmentNumber,
                        historyIndex,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lower + upper) / 2 + 1,
                    upper,
                    segmentNumber,
                    historyIndex,
                    historyTable);
        }
    }

    /**
     * TODO: shivesh
     * @param segmentsToSeal
     * @param newRanges
     * @param segmentTable
     * @param segmentIndex
     * @return
     */
    public static boolean isScaleInputValid(final List<Integer> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final byte[] segmentTable,
                                            final byte[] segmentIndex) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segment -> SegmentRecord.readRecord(segmentTable, segmentIndex, segment).map(x ->
                        new AbstractMap.SimpleEntry<>(x.getRoutingKeyStart(), x.getRoutingKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    private static Map<Segment, Integer> transform(byte[] segmentTable, byte[] segmentIndex, Map<Integer, Integer> epochStreamCutMap) {
        return epochStreamCutMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> getSegment(entry.getKey(), segmentTable, segmentIndex),
                        Map.Entry::getValue));
    }

    private static Map<Integer, Integer> computeEpochCutMap(byte[] historyTable, byte[] historyIndex, byte[] segmentTable,
                                                            byte[] segmentIndex, Map<Integer, Long> streamCut) {
        Map<Integer, Integer> epochStreamCutMap = new HashMap<>();

        int mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        Segment mostRecentSegment = getSegment(mostRecent, segmentTable, segmentIndex);

        final Optional<HistoryRecord> highEpochRecord = HistoryRecord.readRecord(historyTable, historyIndex,
                mostRecentSegment.getEpoch(), false);

        List<Integer> toFind = new ArrayList<>(streamCut.keySet());
        Optional<HistoryRecord> epochRecord = highEpochRecord;

        while (epochRecord.isPresent() && !toFind.isEmpty()) {
            List<Integer> epochSegments = epochRecord.get().getSegments();
            Map<Boolean, List<Integer>> group = toFind.stream().collect(Collectors.groupingBy(epochSegments::contains));
            toFind = Optional.ofNullable(group.get(false)).orElse(Collections.emptyList());
            int epoch = epochRecord.get().getEpoch();
            List<Integer> found = Optional.ofNullable(group.get(true)).orElse(Collections.emptyList());
            found.forEach(x -> epochStreamCutMap.put(x, epoch));
            epochRecord = HistoryRecord.fetchPrevious(epochRecord.get(), historyTable, historyIndex);
        }

        return epochStreamCutMap;
    }

    private static Set<Integer> computeToDelete(Map<Segment, Integer> epochCutMap, byte[] historyTable, byte[] historyIndex,
                                                byte[] segmentTable, byte[] segmentIndex, Set<Integer> deletedSegments) {
        Set<Integer> toDelete = new HashSet<>();
        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);

        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyTable, historyIndex, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            toDelete.addAll(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentTable, segmentIndex);
                // ignore already deleted segments from todelete
                // toDelete.add(epoch.segment overlaps cut.segment && epoch < cut.segment.epoch)
                return !deletedSegments.contains(epochSegmentNumber) &&
                        epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).collect(Collectors.toSet()));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyTable, historyIndex, true);
        }
        return toDelete;
    }

    private static boolean greaterThan(Map<Segment, Integer> map1, Map<Segment, Integer> map2, Map<Integer, Long> cut1, Map<Integer, Long> cut2) {
        // find overlapping segments in map2 for all segments in map1
        // compare epochs. map1 should have epochs gt or eq its overlapping segments in map2
        return map1.entrySet().stream().allMatch(e1 ->
                map2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().getNumber() == e1.getKey().getNumber() && cut1.get(e1.getKey().getNumber()) < cut2.get(e2.getKey().getNumber()))
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
                // if adjacent (prev.high == next.low) then startUpdate only high
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
}
