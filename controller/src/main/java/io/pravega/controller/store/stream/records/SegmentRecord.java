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
import io.pravega.controller.store.stream.records.serializers.SegmentRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
@Builder
/**
 * Class represents one row/record in SegmentTable.
 * Segment table is chunked into multiple files, each containing #SEGMENT_CHUNK_SIZE records.
 * New segment chunk-name is highest-chunk-name + 1
 * Row: [segment-number, segment-creation-time, routing-key-floor-inclusive, routing-key-ceiling-exclusive]
 */
public class SegmentRecord {
    public static final VersionedSerializer.WithBuilder<SegmentRecord, SegmentRecord.SegmentRecordBuilder>
            SERIALIZER = new SegmentRecordSerializer();

    private final int segmentNumber;
    private final long startTime;
    private final int creationEpoch;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    /**
     * Method to read record for a specific segment number. 
     * @param segmentTable segment table
     * @param number segment number to read
     * @return returns segment record
     */
    static Optional<SegmentRecord> readRecord(final byte[] segmentTable, final byte[] index, final int number) {
        Optional<SegmentIndexRecord> record = SegmentIndexRecord.readRecord(index, number);
        return record.map(segmentIndexRecord -> parse(segmentTable, segmentIndexRecord.getSegmentOffset()));
    }

    /**
     * Method to read last 'n' segments from the segment table. Where n is supplied by the caller.
     * @param segmentTable segment table
     * @param count number of segments to read.
     * @return list of last n segments. If number of segments in the table are less than requested, all are returned.
     */
    static List<SegmentRecord> readLastN(final byte[] segmentTable, final byte[] index, final int count) {
        // get offset and parse
        List<SegmentRecord> result = new ArrayList<>(count);
        Optional<SegmentIndexRecord> offset = SegmentIndexRecord.readLatestRecord(index);
        int last = offset.map(SegmentIndexRecord::getSegmentOffset).orElse(0);
        for (int i = 0; i < count; i++) {
            offset = SegmentIndexRecord.readRecord(index, last - i);
            offset.ifPresent(segmentIndexRecord -> result.add(parse(segmentTable, segmentIndexRecord.getSegmentOffset())));
        }
        return result;
    }

    private static SegmentRecord parse(final byte[] table, final int offset) {
        InputStream bas = new ByteArrayInputStream(table, offset, table.length - offset);
        try {
            return SERIALIZER.deserialize(bas);
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    byte[] toByteArray() {
        try {
            return SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    public static class SegmentRecordBuilder implements ObjectBuilder<SegmentRecord> {

    }

}
