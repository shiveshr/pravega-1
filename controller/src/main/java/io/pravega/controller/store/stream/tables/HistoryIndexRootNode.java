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

import java.util.LinkedList;
import java.util.List;

public class HistoryIndexRootNode {
    // root can take upto 100k pointers before the need to split.
    // at 100k * 8 = 800kb..

    // Sorted leaves
    private final ImmutableList<Long> leaves;

    public HistoryIndexRootNode(List<Long> leaves) {
        this.leaves = ImmutableList.copyOf(leaves);
    }

    public static HistoryIndexRootNode parse(byte[] data) {
    }

    public byte[] toByteArray() {
        return new byte[0];
    }

    // helper method to perform binary search and find the appropriate leaf node which may have the history record.

    // helper method to add a new leaf node record here
    public int findLeafNode(long time) {
        // binary search time to find the index which corresponds to leaf node
    }

    public static HistoryIndexRootNode addNewLeaf(HistoryIndexRootNode rootNode, long time) {
        LinkedList<Long> leaves = new LinkedList<>(rootNode.leaves);
        if (time > rootNode.leaves.get(rootNode.leaves.size() - 1)) {
            leaves.add(time);
        }
        return new HistoryIndexRootNode(leaves);
    }
}
