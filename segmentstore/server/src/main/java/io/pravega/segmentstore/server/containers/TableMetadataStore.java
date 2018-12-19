/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link MetadataStore} implementation that stores all Segment Information as {@link TableEntry} instances in a dedicated
 * Table Segment per Segment Container.
 */
@Slf4j
public class TableMetadataStore extends MetadataStore {
    //region Members
    private final TableStore tableStore;
    private final String metadataSegmentName;
    private final AtomicBoolean initialized;

    //endregion

    /**
     * Creates a new instance of the {@link TableMetadataStore} class.
     *
     * @param connector  A {@link MetadataStore.Connector} object that can be used to communicate between the
     *                   {@link MetadataStore} and upstream callers.
     * @param tableStore A {@link TableStore} to use.
     * @param executor   The executor to use for async operations.
     */
    public TableMetadataStore(Connector connector, @NonNull TableStore tableStore, Executor executor) {
        super(connector, executor);
        this.tableStore = tableStore;
        this.metadataSegmentName = StreamSegmentNameUtils.getMetadataSegmentName(connector.getContainerMetadata().getContainerId());
        this.initialized = new AtomicBoolean(false);
    }

    //region MetadataStore Implementation
    @Override
    CompletableFuture<Void> initialize(Duration timeout) {
        Preconditions.checkState(!this.initialized.get(), "TableMetadataStore is already initialized.");

        // Use the TableStore to create the metadata segment. Due to the wiring in StreamSegmentContainer, this
        // call will return to us via a createSegment(this.metadataSegmentName, attributes, timeout). This is important,
        // as the TableStore generates some TableSegment-specific attributes that we cannot guess, and this is the only
        // way to intercept them.
        return this.tableStore.createSegment(this.metadataSegmentName, timeout)
                              .thenRun(() -> this.initialized.set(true));
    }

    @Override
    CompletableFuture<Void> createSegment(String segmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        if (this.initialized.get()) {
            // Already initialized. Delegate to the functionality in the parent class.
            // But first, make sure we don't try to create the Metadata Segment - it is reserved.
            Preconditions.checkArgument(!this.metadataSegmentName.equals(segmentName),
                    "Cannot create Metadata Segment if already initialized.");

            return super.createSegment(segmentName, attributes, timeout);
        } else {
            // Pre-initialization, the only segment we are allowed to create is the Metadata Segment.
            Preconditions.checkArgument(segmentName.equals(this.metadataSegmentName),
                    "Only Metadata Segment can be created if not initialized.");

            // Invoke submitAssignment(), which will ensure that the Metadata Segment is mapped in memory and pinned.
            // If this is the first time we initialize the TableMetadataStore for this SegmentContainer, a new id will be
            // assigned to it.
            // TODO: sanity check: invoke Storage.getInfo on this and handle it already existing.
            // TODO: submitAssignment may throw MetadataUpdateException if we invoke this concurrently.
            return submitAssignment(SegmentInfo.newSegment(this.metadataSegmentName, attributes), true, timeout)
                    .thenAccept(segmentId -> {
                        log.info("{}: Metadata Segment created. Name = '{}', Id = '{}'", this.metadataSegmentName, segmentId);
                        this.initialized.set(true);
                    });
        }
    }

    @Override
    protected CompletableFuture<Void> createSegment(String segmentName, ArrayView segmentInfo, TimeoutTimer timer) {
        ensureInitialized();
        TableEntry entry = TableEntry.notExists(getTableKey(segmentName), segmentInfo);
        return this.tableStore
                .put(this.metadataSegmentName, Collections.singletonList(entry), timer.getRemaining())
                .handle((ignored, ex) -> {
                    if (ex != null) {
                        if (Exceptions.unwrap(ex) instanceof BadKeyVersionException) {
                            ex = new StreamSegmentExistsException(segmentName);
                        }
                        throw new CompletionException(ex);
                    }
                    return null;
                });
    }

    @Override
    public CompletableFuture<Boolean> clearSegmentInfo(String segmentName, Duration timeout) {
        return applyToSegment(
                segmentName,
                (entry, t2) -> this.tableStore
                        .remove(this.metadataSegmentName, Collections.singleton(TableKey.unversioned(entry.getKey().getKey())), t2)
                        .thenApply(v -> true),
                () -> CompletableFuture.completedFuture(false),
                timeout);
    }

    @Override
    protected CompletableFuture<ArrayView> getSegmentInfoInternal(String segmentName, Duration timeout) {
        return applyToSegment(
                segmentName,
                (entry, t2) -> CompletableFuture.completedFuture(entry.getValue()),
                () -> Futures.failedFuture(new StreamSegmentNotExistsException(segmentName)),
                timeout);
    }

    private <T> CompletableFuture<T> applyToSegment(String segmentName, BiFunction<TableEntry, Duration, CompletableFuture<T>> ifExists,
                                                    Supplier<CompletableFuture<T>> ifNotExists, Duration timeout) {
        ensureInitialized();
        ArrayView key = getTableKey(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.tableStore
                .get(this.metadataSegmentName, Collections.singletonList(key), timer.getRemaining())
                .thenComposeAsync(existingData -> {
                    assert existingData.size() == 1 : "Expecting only one result";
                    if (existingData.get(0) == null) {
                        // We don't know anything about this Segment.
                        return ifNotExists.get();
                    }

                    // We have an entry.
                    return ifExists.apply(existingData.get(0), timer.getRemaining());
                }, this.executor);
    }

    @Override
    protected CompletableFuture<Void> updateSegmentInfo(String segmentName, ArrayView segmentInfo, Duration timeout) {
        ensureInitialized();
        TableEntry entry = TableEntry.unversioned(getTableKey(segmentName), segmentInfo);
        return this.tableStore.put(this.metadataSegmentName, Collections.singletonList(entry), timeout).thenRun(Runnables.doNothing());
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.initialized.get(), "TableMetadataStore is not initialized.");
    }

    private ArrayView getTableKey(String segmentName) {
        return new ByteArraySegment(segmentName.getBytes(Charsets.UTF_8));
    }
}
