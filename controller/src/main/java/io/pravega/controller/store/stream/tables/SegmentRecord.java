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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.tables.serializers.SegmentRecordSerializer;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;

@Data
@Builder
/**
 * Class represents one row/record in SegmentTable. Since segment table records are versioned, we can have variable sized
 * records. So we maintain a segment index which identifies start offset for each row in the table.
 */
public class SegmentRecord {
    public static final SegmentRecordSerializer SERIALIZER = new SegmentRecordSerializer();

    private final int segmentNumber;
    private final int creationEpoch;
    private final long creationTime;
    private final double keyStart;
    private final double keyEnd;

    public static class SegmentRecordBuilder implements ObjectBuilder<SegmentRecord> {

    }

    public long segmentId() {
        return StreamSegmentNameUtils.computeSegmentId(segmentNumber, creationEpoch);
    }

    public boolean overlaps(final SegmentRecord segment) {
        return segment.getKeyStart() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    public static boolean overlaps(final AbstractMap.SimpleEntry<Double, Double> first,
                                   final AbstractMap.SimpleEntry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }

    @VisibleForTesting
    public static SegmentRecord newSegmentRecord(int num, int epoch, long time, double start, double end) {
        return SegmentRecord.builder().segmentNumber(num).creationEpoch(epoch).creationTime(time).keyStart(start).keyEnd(end).build();
    }

}
