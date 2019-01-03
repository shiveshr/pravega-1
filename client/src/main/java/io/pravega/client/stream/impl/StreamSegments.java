/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.hash.HashHelper;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The segments that within a stream at a particular point in time.
 */
@EqualsAndHashCode
@Slf4j
public class StreamSegments {
    static final ConcurrentHashMap<Segment, Pair<Double, Double>> SEGMENTS = new ConcurrentHashMap<>();

    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");
    private final NavigableMap<Double, Segment> segments;
    @Getter
    private final String delegationToken;

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     * @param delegationToken Delegation token to access the segments in the segmentstore
     */
    public StreamSegments(NavigableMap<Double, Segment> segments, String delegationToken) {
        this.segments = Collections.unmodifiableNavigableMap(segments);
        this.delegationToken = delegationToken;
        verifySegments();
        AtomicDouble start = new AtomicDouble(0.0);
        segments.entrySet().stream().sorted(Comparator.comparingDouble(Entry::getKey))
                .forEach(x -> {
                    SEGMENTS.putIfAbsent(x.getValue(), new ImmutablePair<>(start.get(), x.getKey()));
                    start.set(x.getKey());
                });
    }

    private void verifySegments() {
        if (!segments.isEmpty()) {
            Preconditions.checkArgument(segments.firstKey() > 0.0, "Nonsense value for segment.");
            Preconditions.checkArgument(segments.lastKey() >= 1.0, "Last segment missing.");
            Preconditions.checkArgument(segments.lastKey() < 1.00001, "Segments should only go up to 1.0");
        }
    }
    
    public Segment getSegmentForKey(String key) {
        return getSegmentForKey(HASHER.hashToRange(key));
    }

    public Segment getSegmentForKey(double key) {
        Preconditions.checkArgument(key >= 0.0);
        Preconditions.checkArgument(key <= 1.0);
        Segment segment = segments.ceilingEntry(key).getValue();
        Pair<Double, Double> range = SEGMENTS.get(segment);
        if (range.getKey() > key || range.getValue() < key) {
            log.error("ClientExpt:: Misrouting for key {} into segment {} range {}", key, segment, range);
            throw new IllegalStateException("Misrouting for key " + key + " into segment " + segment.getSegmentId() + " range [" + range.getKey() + "," + range.getValue() + ")");
        }
        return segment;
    }

    public Collection<Segment> getSegments() {
        return segments.values();
    }
    
    public StreamSegments withReplacementRange(Segment replacedSegment, StreamSegmentsWithPredecessors replacementRanges) {
        log.info("ClientExpt:: replaced segment {} with replacement ranges {}", replacedSegment, replacementRanges);
        Preconditions.checkState(segments.containsValue(replacedSegment), "Segment to be replaced should be present in the segment list");
        verifyReplacementRange(replacementRanges);
        NavigableMap<Double, Segment> result = new TreeMap<>();
        Map<Long, List<SegmentWithRange>> replacedRanges = replacementRanges.getReplacementRanges();
        List<SegmentWithRange> replacements = replacedRanges.get(replacedSegment.getSegmentId());

        replacements.forEach(segmentWithRange -> {
            SEGMENTS.get(segmentWithRange.getSegment());
            SEGMENTS.compute(segmentWithRange.getSegment(),
                    (x, y) -> {
                        if (y != null) {
                            log.info("ClientExpt:: segment {} exists in the map with range [{}, {})", x, y.getKey(), y.getValue());
                            if (y.getKey() != segmentWithRange.getLow() || y.getValue() != segmentWithRange.getHigh()) {
                                log.error("ClientExpt:: New segment range does not match exiting range with client!! {} [{}, {})", y, segmentWithRange.getLow(), segmentWithRange.getHigh());
                            }
                        }

                        log.info("ClientExpt:: adding segment {} with range [{}, {})", segmentWithRange.getSegment(), segmentWithRange.getLow(), segmentWithRange.getHigh());

                        return new ImmutablePair<>(segmentWithRange.getLow(), segmentWithRange.getHigh());
                    });
        });
        
        verifyReplacementSegments(replacedSegment.getSegmentId(), SEGMENTS.get(replacedSegment), replacements);

        Preconditions.checkNotNull(replacements, "Empty set of replacements for: {}", replacedSegment.getSegmentId());
        replacements.sort(Comparator.comparingDouble(SegmentWithRange::getHigh).reversed());
        Segment lastSegmentValue = null;
        for (Entry<Double, Segment> existingEntry : segments.descendingMap().entrySet()) { // iterate from the highest key.
            final Segment existingSegment = existingEntry.getValue();
            if (existingSegment.equals(lastSegmentValue)) {
                // last value was the same segment, it can be consolidated.
                continue;
            }
            if (existingSegment.equals(replacedSegment)) { // Segment needs to be replaced.
                // Invariant: The successor segment(s)'s range should be limited to the replaced segment's range, thereby
                // ensuring that newer writes to the successor(s) happen only for the replaced segment's range.
                for (SegmentWithRange segmentWithRange : replacements) {                        
                    result.put(Math.min(segmentWithRange.getHigh(), existingEntry.getKey()), segmentWithRange.getSegment());
                }
            } else {
                // update remaining values.
                result.put(existingEntry.getKey(), existingEntry.getValue());
            }
            lastSegmentValue = existingSegment; // update lastSegmentValue to reduce number of entries in the map.
        }
        return new StreamSegments(result, delegationToken);
    }

    private void verifyReplacementSegments(long replacedId, Pair<Double, Double> replaced, List<SegmentWithRange> replacements) {
        List<SegmentWithRange> replacementSegments = replacements.stream()
                                                                 .sorted(Comparator.comparingDouble(SegmentWithRange::getLow))
                                                                 .collect(Collectors.toList());

        SegmentWithRange previous = null;
        double lowest = 0.0;
        double highest = 0.0;
        for (SegmentWithRange segment: replacementSegments) {
           if (previous == null) {
               previous = segment;
               lowest = segment.getLow();
               highest = segment.getHigh();
               continue;
           }
           if (segment.getLow() != previous.getHigh()) {
               log.error("ClientExpt:: Invalid successor response from controller. Either disjoint or overlapping. replaced = {}, replacements = {}", 
                       replaced, replacements);

               throw new RuntimeException("Invalid successor response from controller. Either disjoint or overlapping." +
                       " previous high =" + previous.getHigh() + "next low = " + segment.getLow());
           }
           highest = segment.getHigh();
        }
        
        if (replaced.getKey() < lowest || replaced.getValue() > highest) {
            log.error("ClientExpt:: Invalid successor response from controller. Full range not replaced. replaced = {}, replacements = {}",
                    replaced, replacements);

            throw new RuntimeException("invalid successor response. Full range of replaced segment " + replacedId + " is not covered");
        }
    }

    /**
     * Checks that replacementSegments provided are consistent with the segments that currently being used.
     * @param replacementSegments The StreamSegmentsWithPredecessors to verify
     */
    private void verifyReplacementRange(StreamSegmentsWithPredecessors replacementSegments) {
        log.debug("Verification of replacement segments {} with the current segments {}", replacementSegments, segments);
        Map<Long, List<SegmentWithRange>> replacementRanges = replacementSegments.getReplacementRanges();
        for (Entry<Long, List<SegmentWithRange>> ranges : replacementRanges.entrySet()) {
            double lowerReplacementRange = 1;
            double upperReplacementRange = 0;
            for (SegmentWithRange range : ranges.getValue()) {
                upperReplacementRange = Math.max(upperReplacementRange, range.getHigh());
                lowerReplacementRange = Math.min(lowerReplacementRange, range.getLow());
            }
            Entry<Double, Segment> upperReplacedSegment = segments.floorEntry(upperReplacementRange);
            Entry<Double, Segment> lowerReplacedSegment =  segments.higherEntry(lowerReplacementRange);
            Preconditions.checkArgument(upperReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
            Preconditions.checkArgument(lowerReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
         }
    }

    @Override
    public String toString() {
        return "StreamSegments:" + segments.toString();
    }
}
