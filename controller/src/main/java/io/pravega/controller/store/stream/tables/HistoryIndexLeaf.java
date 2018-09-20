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
import io.pravega.common.Exceptions;

import java.util.LinkedList;
import java.util.List;

/**
 * B+ tree "key".
 *
 */
public class HistoryIndexLeaf {
    // 10k records per history index leaf
    private final ImmutableList<HistoryIndexRecord> records;

    public HistoryIndexLeaf(List<HistoryIndexRecord> records) {
        this.records = ImmutableList.copyOf(records);
    }

    public static HistoryIndexLeaf parse(byte[] data) {
    }

    public byte[] toByteArray() {

    }

    // helper method to perform binary search
    public HistoryIndexRecord find(long timestamp) {

    }

    public static HistoryIndexLeaf addRecord(HistoryIndexLeaf leaf, HistoryIndexRecord indexRecord) {
        List<HistoryIndexRecord> records = new LinkedList<>(leaf.records);
        if (leaf.records.stream().noneMatch(x -> x.getTime() > indexRecord.getTime())) {
            records.add(indexRecord);
        }
        return new HistoryIndexLeaf(records);
    }
}
