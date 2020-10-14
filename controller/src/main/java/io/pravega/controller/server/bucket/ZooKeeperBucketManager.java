/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.controller.util.RetryHelper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class ZooKeeperBucketManager extends BucketManager {
    public static final Random RANDOM = new Random();
    private final ZookeeperBucketStore bucketStore;
    private final ConcurrentMap<BucketStore.ServiceType, PathChildrenCache> bucketOwnershipCacheMap;
    private final Supplier<Integer> clusterSizeSupplier;
    private final String processId;

    ZooKeeperBucketManager(String processId, ZookeeperBucketStore bucketStore, BucketStore.ServiceType serviceType, ScheduledExecutorService executor,
                           Function<Integer, BucketService> bucketServiceSupplier, Supplier<Integer> clusterSizeSupplier) {
        super(serviceType, executor, bucketServiceSupplier);
        bucketOwnershipCacheMap = new ConcurrentHashMap<>();
        this.bucketStore = bucketStore;
        this.clusterSizeSupplier = clusterSizeSupplier;
        this.processId = processId;
    }

    @Override
    protected int getBucketCount() {
        return bucketStore.getBucketCount(getServiceType());
    }

    @Override
    public void startBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.computeIfAbsent(getServiceType(),
                x -> bucketStore.getServiceOwnershipPathChildrenCache(getServiceType()));

        PathChildrenCacheListener bucketListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // evaluate if its load factor has changed and if it should rebalance.
                    handleChildAdded();
                    
                    break;
                case CHILD_REMOVED:
                    int clusterSize = clusterSizeSupplier.get();
                    int load = getBucketServices().size();
                    int idealLoad = bucketStore.getBucketCount(getServiceType()) / clusterSize + 1;
                    long delay = load < idealLoad ? 0L : RANDOM.nextInt(load - idealLoad) * 1000L;
                    int bucketId = Integer.parseInt(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    Futures.delayedFuture(() -> RetryHelper.withIndefiniteRetriesAsync(() -> tryTakeOwnership(bucketId),
                            e -> log.warn("{}: exception while attempting to take ownership for bucket {} ", getServiceType(),
                                    bucketId, e.getMessage()), getExecutor()), delay, getExecutor());
                    break;
                case CONNECTION_LOST:
                    log.warn("{}: Received connectivity error", getServiceType());
                    break;
                default:
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), getServiceType());
            }
        };

        pathChildrenCache.getListenable().addListener(bucketListener);
        log.info("bucket ownership listener registered on bucket root {}", getServiceType());

        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        } catch (Exception e) {
            log.error("Starting ownership listener for service {} threw exception", getServiceType(), e);
            throw Exceptions.sneakyThrow(e);
        }

    }

    private void handleChildAdded() {
        int clusterSize = clusterSizeSupplier.get();
        int load = getBucketServices().size();
        int idealLoad = bucketStore.getBucketCount(getServiceType()) / clusterSize + 1;
        if (idealLoad < load) {
            Futures.allOfWithResults(getBucketServices()
                    .entrySet().stream().sorted(Comparator.comparingInt(x -> x.getValue().getKnownStreams().size()))
                    .collect(Collectors.toList()).subList(idealLoad, load)
                    .stream().map(x -> stopBucketService(x.getKey())
                            // only after we stop the bucket service will we release the ownership. 
                            // this will trigger other controller instances to receive child removed notification
                            // and they will each independently attempt to acquire ownership of released bucket. 
                            .thenCompose(v -> bucketStore.releaseBucketOwnership(getServiceType(), getBucketCount(), processId)))
                    .collect(Collectors.toList()));
        }
    }

    @Override
    public void stopBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.remove(getServiceType());
        if (pathChildrenCache != null) {
            try {
                pathChildrenCache.clear();
                pathChildrenCache.close();
            } catch (IOException e) {
                log.warn("unable to close listener for bucket ownership", e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> initializeService() {
        return bucketStore.createBucketsRoot(getServiceType());
    }

    @Override
    public CompletableFuture<Void> initializeBucket(int bucket) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount(getServiceType()));
        
        return bucketStore.createBucket(getServiceType(), bucket);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, Executor executor) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount(getServiceType()));
        return bucketStore.takeBucketOwnership(getServiceType(), bucket, processId);
    }
}
