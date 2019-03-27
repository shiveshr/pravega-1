/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Builder
@Data
@AllArgsConstructor
@Slf4j
public class CommitEvent implements ControllerEvent {
    private static final long serialVersionUID = 1L;
    private static final AtomicLong counterShivesh = new AtomicLong();
    private final String scope;
    private final String stream;
    private final int epoch;
    private final long shivesh;

    public CommitEvent(String scope, String stream, int epoch) {
        this(scope, stream, epoch, counterShivesh.incrementAndGet());
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        log.info("shivesh:: commit event {} processing started on abstract request processor", shivesh);
        return processor.processCommitTxnRequest(this);
    }

    //region Serialization

    private static class CommitEventBuilder implements ObjectBuilder<CommitEvent> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<CommitEvent, CommitEventBuilder> {
        @Override
        protected CommitEventBuilder newBuilder() {
            return CommitEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CommitEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.stream);
            target.writeCompactInt(e.epoch);
            target.writeLong(e.shivesh);
        }

        private void read00(RevisionDataInput source, CommitEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.stream(source.readUTF());
            b.epoch(source.readCompactInt());
            b.shivesh(source.readLong());
        }
    }

    //endregion
}
