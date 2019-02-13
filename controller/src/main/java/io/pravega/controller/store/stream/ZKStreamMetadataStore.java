/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.util.Config;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * ZK stream metadata store.
 */
@Slf4j
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    static final String DELETED_STREAMS_PATH = "/lastActiveStreamSegment/%s";
    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";
    static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    static final String COMPLETED_TX_BATCH_ROOT_PATH = COMPLETED_TX_ROOT_PATH + "/batches";
    static final String COMPLETED_TX_BATCH_PATH = COMPLETED_TX_BATCH_ROOT_PATH + "/%d";
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ZKStoreHelper storeHelper;

    private final ZKGarbageCollector completedTxnGC;
    private final ZkInt96Counter counter;
    
    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, Executor executor) {
        this(client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }

    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, Executor executor, Duration gcPeriod) {
        super(new ZKHostIndex(client, "/hostTxnIndex", executor));
        storeHelper = new ZKStoreHelper(client, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, storeHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.counter = new ZkInt96Counter(storeHelper);    
    }

    private CompletableFuture<Void> gcCompletedTxn() {
        return storeHelper.getChildren(COMPLETED_TX_BATCH_ROOT_PATH)
                .thenApply(children -> {
                            // retain latest two and delete remainder.
                            List<Long> list = children.stream().map(Long::parseLong).sorted().collect(Collectors.toList());
                            if (list.size() > 2) {
                                return list.subList(0, list.size() - 2);
                            } else {
                                return new ArrayList<Long>();
                            }
                        }
                )
                .thenCompose(toDeleteList -> {
                    log.debug("deleting batches {} on new scheme" + toDeleteList);

                    // delete all those marked for toDelete.
                    return Futures.allOf(toDeleteList.stream()
                            .map(toDelete -> storeHelper.deleteTree(String.format(COMPLETED_TX_BATCH_PATH, toDelete)))
                            .collect(Collectors.toList()));
                });
    }

    @Override
    ZKStream newStream(final String scope, final String name) {
        return new ZKStream(scope, name, storeHelper, completedTxnGC::getLatestBatch);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return counter.getNextCounter();
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }
    
    @Override
    ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return storeHelper.checkExists(String.format("/store/%s", scopeName))
                .thenApply(scopeExists -> {
                    if (scopeExists) {
                        return scopeName;
                    } else {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        ZKStream stream = newStream(scopeName, streamName);
        return storeHelper.checkExists(stream.getStreamPath());
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return storeHelper.getData(String.format(DELETED_STREAMS_PATH, getScopedStreamName(scopeName, streamName)))
                          .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return BitConverter.readInt(data.getData(), 0) + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String deletePath = String.format(DELETED_STREAMS_PATH, getScopedStreamName(scope, stream));
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return storeHelper.getData(deletePath)
                          .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          })
                          .thenCompose(data -> {
                              log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                              if (data == null) {
                                  return Futures.toVoid(storeHelper.createZNodeIfNotExist(deletePath, maxSegmentNumberBytes));
                              } else {
                                  final int oldLastActiveSegment = BitConverter.readInt(data.getData(), 0);
                                  Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                          "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                          oldLastActiveSegment, scope, stream, lastActiveSegment);
                                  return Futures.toVoid(storeHelper.setData(deletePath, new Data(maxSegmentNumberBytes, data.getVersion())));
                              }
                          });
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return Base64.getEncoder().encodeToString(scopedStreamName.getBytes());
    }

    private String decodedScopedStreamName(String encodedScopedStreamName) {
        return new String(Base64.getDecoder().decode(encodedScopedStreamName));
    }

    private StreamImpl getStreamFromPath(String path) {
        String scopedStream = decodedScopedStreamName(ZKPaths.getNodeFromPath(path));
        String[] splits = scopedStream.split("/");
        return new StreamImpl(splits[0], splits[1]);
    }
    
    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }
    // endregion
}
