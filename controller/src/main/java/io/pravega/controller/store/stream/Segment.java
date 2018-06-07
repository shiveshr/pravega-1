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

import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Data;
import lombok.ToString;

import java.util.AbstractMap;

/**
 * Properties of a stream segment that don't change over its lifetime.
 */
@Data
@ToString(includeFieldNames = true)
public class Segment {
    private final long segmentId;
    private final long start;
    private final double keyStart;
    private final double keyEnd;

    public int getEpoch() {
        return StreamSegmentNameUtils.getEpoch(segmentId);
    }

    public boolean overlaps(final Segment segment) {
        return segment.getKeyEnd() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    public static boolean overlaps(final AbstractMap.SimpleEntry<Double, Double> first,
                                   final AbstractMap.SimpleEntry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }
}
