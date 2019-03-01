/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.fault.FailoverSweeper;
import com.google.common.base.Preconditions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.UNCONDITIONAL_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetries;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

@Slf4j
public class TaskSweeper implements FailoverSweeper {

    private final StreamMetadataStore metadataStore;
    private final String hostId;
    private final ScheduledExecutorService executor;
    private final StreamMetadataTasks metadataTasks;
    
    public TaskSweeper(final StreamMetadataStore metadataStore, final String hostId,
                       final ScheduledExecutorService executor, final StreamMetadataTasks metadataTasks) {
        this.metadataStore = metadataStore;
        this.hostId = hostId;
        this.executor = executor;
        this.metadataTasks = metadataTasks;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public CompletableFuture<Void> sweepFailedProcesses(final Supplier<Set<String>> runningProcesses) {
        return withRetriesAsync(metadataStore::listHostsWithPendingTask, RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor)
                .thenComposeAsync(registeredHosts -> {
                    log.info("Hosts {} have ongoing tasks", registeredHosts);
                    registeredHosts.removeAll(withRetries(runningProcesses, UNCONDITIONAL_PREDICATE, Integer.MAX_VALUE));
                    log.info("Failed hosts {} have orphaned tasks", registeredHosts);
                    return Futures.allOf(registeredHosts.stream()
                                                        .map(this::handleFailedProcess).collect(Collectors.toList()));
                }, executor);
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method with one of the dead hostId.
     * <p>
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     * @param oldHostId old host id
     * @return A future that completes when sweeping completes
     */
    @Override
    public CompletableFuture<Void> handleFailedProcess(final String oldHostId) {

        log.info("Sweeping orphaned tasks for host {}", oldHostId);
        return withRetriesAsync(() -> Futures.doWhileLoop(
                () -> executeHostTask(oldHostId),
                x -> x != null, executor).whenCompleteAsync((result, ex) ->
                                                     log.info("Sweeping orphaned tasks for host {} complete", oldHostId), executor),
                RETRYABLE_PREDICATE),
                Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<Void> executeHostTask(final String oldHostId) {
        return metadataStore.getPendingsTaskForHost(oldHostId, 100)
                                .thenComposeAsync(tasks -> Futures.allOf(
                                        tasks.entrySet().stream().map(entry -> 
                                                metadataTasks.writeEvent(entry.getValue())
                                                             .thenCompose(v -> 
                                                                     metadataStore.removeTaskFromIndex(oldHostId, entry.getKey())))
                                             .collect(Collectors.toList())), executor);
    }

    /**
     * Internal key used in mapping tables.
     *
     * @param taskName    method name.
     * @param taskVersion method version.,
     * @return key
     */
    private String getKey(final String taskName, final String taskVersion) {
        return taskName + "--" + taskVersion;
    }
}
