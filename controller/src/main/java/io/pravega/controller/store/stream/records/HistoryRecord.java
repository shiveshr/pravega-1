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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;
import io.pravega.controller.store.stream.records.serializers.HistoryRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Lombok;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will use history index for
 * traversal.
 * Row : [epoch][List-of-active-segment-numbers], [scaleTime]
 */
@Data
public class HistoryRecord {
    public static final VersionedSerializer.WithBuilder<HistoryRecord, HistoryRecord.HistoryRecordBuilder>
            SERIALIZER = new HistoryRecordSerializer();

    @Getter
    private final int epoch;
    @Getter
    private final List<Integer> segments;
    @Getter
    private final long scaleTime;
    @Getter
    private final boolean partial;

    @Builder
    HistoryRecord(int epoch, List<Integer> segments) {
        this(epoch, segments, Long.MIN_VALUE);
    }

    @Builder
    HistoryRecord(int epoch, List<Integer> segments, long scaleTime) {
        this.epoch = epoch;
        this.segments = segments;
        this.scaleTime = scaleTime;
        partial = scaleTime == Long.MIN_VALUE;
    }

    public byte[] toByteArray() {
        try {
            return SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    /**
     * Read record from the history table at the specified offset.
     *
     * @param historyTable  history table
     * @param historyIndex  history index
     * @param epoch         epoch to read
     * @param ignorePartial if set, ignore if the record is partial
     * @return
     */
    public static Optional<HistoryRecord> readRecord(final byte[] historyTable, final byte[] historyIndex, final int epoch,
                                                     boolean ignorePartial) {
        Optional<HistoryIndexRecord> historyIndexRecord = HistoryIndexRecord.readRecord(historyIndex, epoch);
        if (!historyIndexRecord.isPresent()) {
            return Optional.empty();
        }

        int offset = historyIndexRecord.get().getHistoryOffset();

        HistoryRecord record = parse(historyTable, offset);

        if (record.isPartial() && ignorePartial) {
            // this is a partial record and we have been asked to ignore it.
            return Optional.empty();
        } else {
            return Optional.of(record);
        }
    }

    /**
     * Return latest record in the history table.
     *
     * @param historyTable  history table
     * @param historyIndex  history index
     * @param ignorePartial Ignore partial entry.
     * @return returns the latest history record. If latest entry is partial entry and ignorePartial flag is true
     * then read the previous entry.
     */
    public static Optional<HistoryRecord> readLatestRecord(final byte[] historyTable, final byte[] historyIndex,
                                                           boolean ignorePartial) {
        Optional<HistoryIndexRecord> latestIndex = HistoryIndexRecord.readLatestRecord(historyIndex);
        if (!latestIndex.isPresent()) {
            return Optional.empty();
        }

        Optional<HistoryRecord> record = readRecord(historyTable, historyIndex, latestIndex.get().getEpoch(), ignorePartial);
        if (!record.isPresent()) {
            // we have the index updated but the history table isnt updated yet. So fetch the previous indexed record.
            record = readRecord(historyTable, historyIndex, latestIndex.get().getEpoch() - 1, ignorePartial);
        }

        if (ignorePartial && record.get().isPartial()) {
            return fetchPrevious(record.get(), historyTable, historyIndex);
        } else {
            return record;
        }
    }

    public static Optional<HistoryRecord> fetchNext(final HistoryRecord record, final byte[] historyTable,
                                                    final byte[] historyIndex, boolean ignorePartial) {
        return readRecord(historyTable, historyIndex, record.epoch + 1, ignorePartial);
    }

    public static Optional<HistoryRecord> fetchPrevious(final HistoryRecord record, final byte[] historyTable, final byte[] historyIndex) {
        return readRecord(historyTable, historyIndex, record.epoch - 1, true);
    }

    private static HistoryRecord parse(final byte[] table, final int offset) {
        InputStream inputStream = new ByteArrayInputStream(table, offset, table.length - offset);
        try {
            return SERIALIZER.deserialize(inputStream);
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    public static List<Pair<Long, List<Integer>>> readAllRecords(byte[] historyTable, byte[] historyIndex) {
        List<Pair<Long, List<Integer>>> result = new LinkedList<>();
        Optional<HistoryRecord> record = readLatestRecord(historyTable, historyIndex,true);
        while (record.isPresent()) {
            result.add(new ImmutablePair<>(record.get().getScaleTime(), record.get().getSegments()));
            record = fetchPrevious(record.get(), historyTable, historyIndex);
        }
        return result;
    }

    public static class HistoryRecordBuilder implements ObjectBuilder<HistoryRecord> {

    }
}
