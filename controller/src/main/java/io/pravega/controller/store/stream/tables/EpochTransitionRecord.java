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
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.tables.serializers.EpochTransitionRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.AbstractMap;

/**
 * Transient record that is created while epoch transition takes place and captures the transition. This record is deleted
 * once transition completes.
 */
@Data
@Builder
@AllArgsConstructor
public class EpochTransitionRecord {
    public static final EpochTransitionRecordSerializer SERIALIZER = new EpochTransitionRecordSerializer();

    /**
     * Active epoch at the time of requested transition.
     */
    final int activeEpoch;
    /**
     * Time when this epoch creation request was started.
     */
    final long time;
    /**
     * Segments to be sealed.
     */
    final ImmutableSet<Long> segmentsToSeal;
    /**
     * Key ranges for new segments to be created.
     */
    ImmutableMap<Long, AbstractMap.SimpleEntry<Double, Double>> newSegmentsWithRange;

    public int getNewEpoch() {
        return activeEpoch + 1;
    }

    public static EpochTransitionRecord createForScale(int activeEpoch, long time, ImmutableSet<Long> segmentsToSeal,
                                                       ImmutableMap<Long, AbstractMap.SimpleEntry<Double, Double>> newSegmentsWithRange) {
        Preconditions.checkArgument(segmentsToSeal != null && !segmentsToSeal.isEmpty());
        Preconditions.checkArgument(newSegmentsWithRange != null && !newSegmentsWithRange.isEmpty());
        return EpochTransitionRecord.builder().activeEpoch(activeEpoch).segmentsToSeal(segmentsToSeal).time(time)
                .newSegmentsWithRange(newSegmentsWithRange).build();
    }

    public static EpochTransitionRecord createForRollingTxn(int activeEpoch, int transactionEpoch, long time) {
        // For rolling transaction, segments to seal and segments to create are determined by epoch and setting segments to
        // seal and segments to create are redundant and hence we set empty sets and maps respectively.
        Preconditions.checkArgument(activeEpoch > transactionEpoch);
        return EpochTransitionRecord.builder().activeEpoch(activeEpoch)
                .segmentsToSeal(ImmutableSet.<Long>builder().build()).time(time)
                .newSegmentsWithRange(ImmutableMap.<Long, AbstractMap.SimpleEntry<Double, Double>>builder().build()).build();
    }

    public static class EpochTransitionRecordBuilder implements ObjectBuilder<EpochTransitionRecord> {

    }

    @SneakyThrows(IOException.class)
    public static EpochTransitionRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
