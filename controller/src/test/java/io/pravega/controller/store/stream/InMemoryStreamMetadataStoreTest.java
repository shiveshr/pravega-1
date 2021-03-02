/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.shared.NameUtils;
import lombok.Synchronized;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryStreamMetadataStoreTest extends StreamMetadataStoreTest {

    @Override
    public void setupStore() throws Exception {
        store = new TestInmemoryStore(executor);
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 1,
                BucketStore.ServiceType.WatermarkingService, 1);
        bucketStore = StreamStoreFactory.createInMemoryBucketStore(map);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
    }

    static class TestInmemoryStore extends InMemoryStreamMetadataStore implements TestStore {
        HashMap<String, Stream> map = new HashMap<>();

        TestInmemoryStore(ScheduledExecutorService executor) {
            super(executor);
        }

        @Override
        @Synchronized
        Stream newStream(String scope, String name, long requestId) {
            String scopedStreamName = NameUtils.getScopedStreamName(scope, name);
            if (map.containsKey(scopedStreamName)) {
                return map.get(scopedStreamName);
            } else {
                return super.newStream(scope, name, requestId);
            }
        }

        @Override
        @Synchronized
        public void setStream(Stream stream) {
            String scopedStreamName = NameUtils.getScopedStreamName(stream.getScope(), stream.getName());
            map.put(scopedStreamName, stream);
        }

        @Override
        public void close() throws IOException {
            map.clear();
            super.close();
        }
    }
}
