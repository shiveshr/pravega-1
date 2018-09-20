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

import io.pravega.common.util.BitConverter;
import lombok.Data;

import java.util.Optional;

@Data
/**
 * Class corresponding to a record/row in Index table.
 * contains time to epoch indx
 */
public class HistoryIndexRecord {
    // TODO: shivesh make this serializable
    private static final int INDEX_RECORD_SIZE = Long.BYTES + Integer.BYTES;
    private final long time;
    private final int epoch;

    public static Optional<HistoryIndexRecord> readRecord() {
    }

    private static HistoryIndexRecord parse(final byte[] bytes, int epoch) {
        int offset = epoch * INDEX_RECORD_SIZE;
        final int historyOffset = BitConverter.readInt(bytes, offset);
        return new HistoryIndexRecord(epoch, historyOffset);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeInt(b, 0, historyOffset);
        BitConverter.writeLong(b, 0, historyOffset);

        return b;
    }
}
