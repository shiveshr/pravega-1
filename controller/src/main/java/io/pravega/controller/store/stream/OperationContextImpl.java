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

import io.pravega.controller.store.VersionedMetadata;
import lombok.Getter;
import java.util.concurrent.ConcurrentHashMap;

class OperationContextImpl implements OperationContext {

    @Getter
    private final Stream stream;

    private final ConcurrentHashMap<String, VersionedMetadata<?>> fetched; 

    OperationContextImpl(Stream stream) {
        this.stream = stream;
        fetched = new ConcurrentHashMap<>();
    }

    @Override
    public void load(String key, VersionedMetadata<?> value) {
        fetched.put(key, value);
    }

    @Override
    public void unload(String key) {
        fetched.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> VersionedMetadata<T> get(String key, Class<T> tClass) {
        return (VersionedMetadata<T>) fetched.get(key);
    }
}
