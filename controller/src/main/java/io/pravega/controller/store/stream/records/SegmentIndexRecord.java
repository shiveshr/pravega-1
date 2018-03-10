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

import io.pravega.common.util.BitConverter;
import lombok.Data;

import java.util.Optional;

@Data
/**
 * Class corresponding to a record/row in SegmentIndex table.
 * Each row is fixed size
 * Row: [offset-in-Segment-table]
 */
public class SegmentIndexRecord {
    private static final int INDEX_RECORD_SIZE = Integer.BYTES;
    private final int segmentNumber;
    private final int segmentOffset;

    public static Optional<SegmentIndexRecord> readRecord(final byte[] indexTable, final int segmentNumber) {
        if ((segmentNumber + 1) * INDEX_RECORD_SIZE > indexTable.length) {
            return Optional.empty();
        } else {
            return Optional.of(parse(indexTable, segmentNumber));
        }
    }

    public static Optional<SegmentIndexRecord> readLatestRecord(final byte[] indexTable) {
        return readRecord(indexTable, indexTable.length / INDEX_RECORD_SIZE);
    }

    private static SegmentIndexRecord parse(final byte[] bytes, int segmentNumber) {
        int offset = segmentNumber * INDEX_RECORD_SIZE;
        final int segmentOffset = BitConverter.readInt(bytes, offset);
        return new SegmentIndexRecord(segmentNumber, segmentOffset);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeInt(b, Integer.BYTES, segmentOffset);

        return b;
    }
}
