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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.tables.serializers.HistoryTimeSeriesRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HistoryTimeSeriesRecord {
    public static final HistoryTimeSeriesRecordSerializer SERIALIZER = new HistoryTimeSeriesRecordSerializer();

    @Getter
    private final int epoch;
    /**
     * The reference epoch is the original epoch that this current epoch duplicates.
     * If referenceEpoch is same as epoch, then this is a clean creation of epoch rather than a duplicate.
     * If we are creating a duplicate of an epoch that was already a duplicate, we set the reference to the parent.
     * This ensures that instead of having a chain of duplicates we have a tree of depth one where all duplicates
     * are children of original epoch as common parent.
     */
    @Getter
    private final int referenceEpoch;

    /**
     * Segment ids have two parts, primary id and secondary id.
     * Primary Id is encoded in LSB of each long and secondary id is encoded in MSB.
     * Note: secondary id is optional and 0 value will signify its absence.
     */
    @Getter
    private final List<SegmentRecord> segmentsSealed;

    @Getter
    private final List<SegmentRecord> segmentsCreated;

    @Getter
    private final long scaleTime;

    @Builder
    HistoryTimeSeriesRecord(int epoch, int referenceEpoch, List<SegmentRecord> segmentsSealed, List<SegmentRecord> segmentsCreated,
                            long creationTime) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segmentsSealed = ImmutableList.copyOf(segmentsSealed);
        this.segmentsCreated = ImmutableList.copyOf(segmentsCreated);
        this.scaleTime = creationTime;
    }

    @Builder
    HistoryTimeSeriesRecord(int epoch, List<SegmentRecord> segmentsSealed, List<SegmentRecord> segmentsCreated, long creationTime) {
        this(epoch, epoch, segmentsSealed, segmentsCreated, creationTime);
    }

    @SneakyThrows(IOException.class)
    public ArrayView toArrayView() {
        return SERIALIZER.serialize(this);
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeriesRecord parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class HistoryTimeSeriesRecordBuilder implements ObjectBuilder<HistoryTimeSeriesRecord> {

    }
}
