/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.util.RetryHelper;
import lombok.Getter;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractBucketService extends AbstractService implements BucketChangeListener {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AbstractBucketService.class));

    @Getter
    final int bucketId;
    final ScheduledExecutorService executor;
    private final BucketStore bucketStore;
    private final BucketStore.ServiceType serviceType;
    private final ConcurrentMap<Stream, CompletableFuture<Void>> workFutureMap;
    private final LinkedBlockingQueue<StreamNotification> notifications;
    private final CompletableFuture<Void> latch;
    private CompletableFuture<Void> notificationLoop;

    AbstractBucketService(BucketStore.ServiceType serviceType, int bucketId, BucketStore bucketStore, ScheduledExecutorService executor) {
        this.serviceType = serviceType;
        this.bucketId = bucketId;
        this.bucketStore = bucketStore;
        this.executor = executor;
        this.notifications = new LinkedBlockingQueue<>();
        this.workFutureMap = new ConcurrentHashMap<>();
        this.latch = new CompletableFuture<>();
    }

    @Override
    protected void doStart() {
        RetryHelper.withIndefiniteRetriesAsync(() -> bucketStore.getStreamsForBucket(serviceType, bucketId, executor)
                        .thenAccept(streams -> workFutureMap.putAll(streams.stream().map(s -> {
                                    String[] splits = s.split("/");
                                    log.info("Adding new stream {}/{} to bucket {} during bootstrap for service {}", splits[0], 
                                            splits[1], bucketId, serviceType);
                                    return new StreamImpl(splits[0], splits[1]);
                                }).collect(Collectors.toMap(s -> s, this::startWork))
                        )),
                e -> log.warn("exception thrown getting streams for bucket {}, e = {}", bucketId, e), executor)
                   .thenAccept(x -> {
                       log.info("streams collected for the bucket {} for service {}, registering for change notification and starting loop " +
                               "for processing notifications", bucketId, serviceType);
                       bucketStore.registerBucketChangeListener(serviceType, bucketId, this);
                   })
                   .whenComplete((r, e) -> {
                       if (e != null) {
                           notifyFailed(e);
                       } else {
                           notifyStarted();
                           notificationLoop = Futures.loop(this::isRunning, this::processNotification, executor);
                       }
                       latch.complete(null);
                   });
    }

    private CompletableFuture<Void> processNotification() {
        return CompletableFuture.runAsync(() -> {
            StreamNotification notification =
                    Exceptions.handleInterruptedCall(() -> notifications.poll(1, TimeUnit.SECONDS));
            if (notification != null) {
                final StreamImpl stream;
                switch (notification.getType()) {
                    case StreamAdded:
                        log.info("New stream {}/{} added to bucket {} for service {}", notification.getScope(), 
                                notification.getStream(), bucketId, serviceType);
                        stream = new StreamImpl(notification.getScope(), notification.getStream());
                        workFutureMap.computeIfAbsent(stream, x -> startWork(stream));
                        break;
                    case StreamRemoved:
                        log.info("Stream {}/{} removed from bucket {} for service {}", notification.getScope(), 
                                notification.getStream(), bucketId, serviceType);
                        stream = new StreamImpl(notification.getScope(), notification.getStream());
                        workFutureMap.remove(stream).cancel(true);
                        break;
                    case StreamUpdated:
                        break;
                    case ConnectivityError:
                        log.info("Bucket service {} StreamNotification for connectivity error", serviceType);
                        break;
                }
            }
        }, executor);
    }
    
    abstract CompletableFuture<Void> startWork(StreamImpl stream);

    @Override
    protected void doStop() {
        log.info("Stop request received for bucket {} for service {}", bucketId, serviceType);
        Futures.await(latch);
        if (notificationLoop != null) {
            notificationLoop.thenAccept(x -> {
                // cancel all retention futures
                workFutureMap.forEach((key, value) -> value.cancel(true));
                bucketStore.unregisterBucketChangeListener(serviceType, bucketId);
            }).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Error while stopping bucket {} for service {} {}", bucketId, serviceType, e);
                    notifyFailed(e);
                } else {
                    log.info("Cancellation for all background work for bucket {} for service {} issued", bucketId, serviceType);
                    notifyStopped();
                }
            });
        } else {
            notifyStopped();
        }
    }

    @Override
    public void notify(StreamNotification notification) {
        notifications.add(notification);
    }
    
    @VisibleForTesting
    Map<Stream, CompletableFuture<Void>> getWorkFutureMap() {
        return Collections.unmodifiableMap(workFutureMap);
    }
}
