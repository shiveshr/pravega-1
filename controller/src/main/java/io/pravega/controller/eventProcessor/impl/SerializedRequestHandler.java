/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * SerializedRequestHandler class is used to serialize requests for a key and process them.
 * It maintains a map of key and its work queue.
 * Any new request received for the key is queued up if queue is not empty. If a worker queue for the key is not found in map,
 * a new queue is created and the event is put in the queue and this is added to the worker map.
 * The processing is then scheduled asynchronously for the key.
 *
 * Once all pending processing for a key ends, the key is removed from the work map the moment its queue becomes empty.
 */
@AllArgsConstructor
@Slf4j
public abstract class SerializedRequestHandler<T extends ControllerEvent> implements RequestHandler<T> {

    protected final ScheduledExecutorService executor;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, ConcurrentLinkedQueue<Work>> workers = new HashMap<>();

    @Override
    public final CompletableFuture<Void> process(final T streamEvent) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Work work = new Work(streamEvent, System.currentTimeMillis(), result);
        String key = streamEvent.getKey();

        final ConcurrentLinkedQueue<Work> queue;

        synchronized (lock) {
            if (streamEvent instanceof CommitEvent) {
                log.info("shivesh:: adding next event to the queue {}", ((CommitEvent) streamEvent).getShivesh());
            }
            if (workers.containsKey(key)) {
                workers.get(key).add(work);
                queue = null;
            } else {
                queue = new ConcurrentLinkedQueue<>();
                queue.add(work);
                workers.put(key, queue);
            }
            log.info("shivesh:: queue size is {}", workers.get(streamEvent.getKey()).size());
        }

        if (queue != null) {
            log.info("shivesh:: new work with null queue.. calling execute on the queue for stream", streamEvent.getKey());

            executor.execute(() -> run(key, queue));
        }

        return result;
    }

    public abstract CompletableFuture<Void> processEvent(final T event);

    public boolean toPostpone(final T event, final long pickupTime, final Throwable exception) {
        return false;
    }


    /**
     * Run method is called only if work queue is not empty. So we can safely do a workQueue.poll.
     * WorkQueue.poll should only happen in the run method and no where else.
     *
     * @param key       key for which we want to process the next event
     * @param workQueue work queue for the key
     */
    private void run(String key, ConcurrentLinkedQueue<Work> workQueue) {
        Work work = workQueue.poll();
        CompletableFuture<Void> future;
        try {
            log.info("shivesh:: starting event {} processing", work.event);
            future = processEvent(work.getEvent());
        } catch (Exception e) {
            log.info("shivesh:: adding synchronous error catch actually helped");
            future = Futures.failedFuture(e);
        }

        future.handle((r, e) -> {
            if (e != null && toPostpone(work.getEvent(), work.getPickupTime(), e)) {
                log.info("shivesh:: postponing event {} processing", work.event);
                handleWorkPostpone(key, workQueue, work);
            } else {
                if (e != null) {
                    log.info("shivesh:: workflow {} failed with exception:{}", work.event, Exceptions.unwrap(e).getClass());
                    work.getResult().completeExceptionally(e);
                } else {
                    log.info("shivesh:: workflow completed for event {}", work.event);
                    work.getResult().complete(r);
                }

                handleWorkComplete(key, workQueue, work);
            }
            return null;
        });
    }

    private void handleWorkPostpone(String key, ConcurrentLinkedQueue<Work> workQueue, Work work) {
        // if the request handler decides to postpone the processing,
        // put the work at the back of the queue to be picked again.
        // Note: we have not completed the work's result future here.
        // Since there is at least one event in the queue (we just
        // added) so we will call run again.
        synchronized (lock) {
            workers.get(key).add(work);
        }

        log.info("shivesh:: postponement:: calling execute on the queue for stream ", work.event.getKey());

        executor.execute(() -> run(key, workQueue));
    }

    private void handleWorkComplete(String key, ConcurrentLinkedQueue<Work> workQueue, Work work) {
        work.getResult().whenComplete((rw, ew) -> {
            boolean toExecute = false;
            synchronized (lock) {
                if (workQueue.isEmpty()) {
                    workers.remove(key);
                } else {
                    toExecute = true;
                }
            }

            if (toExecute) {
                log.info("shivesh:: work complete:: calling execute on the queue for stream ", work.event.getKey());
                executor.execute(() -> run(key, workQueue));
            }
        });
    }

    @VisibleForTesting
    List<Pair<T, CompletableFuture<Void>>> getEventQueueForKey(String key) {
        List<Pair<T, CompletableFuture<Void>>> retVal = null;

        synchronized (lock) {
            if (workers.containsKey(key)) {
                retVal = workers.get(key).stream().map(x -> new ImmutablePair<>(x.getEvent(), x.getResult())).collect(Collectors.toList());
            }
        }

        return retVal;
    }

    @Data
    private class Work {
        private final T event;
        private final long pickupTime;
        private final CompletableFuture<Void> result;
    }

}
