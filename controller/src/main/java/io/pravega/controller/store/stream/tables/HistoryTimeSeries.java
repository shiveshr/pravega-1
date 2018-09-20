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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.Segment;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will use history index for
 * traversal.
 * Row : [epoch][List-of-active-segment-numbers], [scaleTime]
 */
@Data
public class HistoryTimeSeries {
    public static final HistoryTimeSeriesSerializer SERIALIZER = new HistoryTimeRecordSerializer();

    private final List<HistoryTimeSeriesRecord> historyRecords;

    @Builder
    HistoryTimeSeries(List<HistoryTimeSeriesRecord> historyRecords) {
        this.historyRecords = new LinkedList<>();
        this.historyRecords.addAll(historyRecords);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeries parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class HistoryTimeSeriesBuilder implements ObjectBuilder<HistoryTimeSeries> {

    }

    public ImmutableList<HistoryTimeSeriesRecord> getHistoryRecords() {
        return ImmutableList.copyOf(historyRecords);
    }

    public static HistoryTimeSeries addHistoryRecord(HistoryTimeSeries series, HistoryTimeSeriesRecord record) {
        List<HistoryTimeSeriesRecord> list = Lists.newArrayList(series.historyRecords);

        // add only if cut.recordingTime is newer than any previous cut
        if (list.stream().noneMatch(x -> x.getScaleTime() > record.getScaleTime())) {
            list.add(record);
        }

        return new HistoryTimeSeries(list);
    }
}
