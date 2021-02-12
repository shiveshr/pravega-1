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

import io.pravega.controller.store.Scope;
import lombok.Getter;

class OperationContextImpl implements OperationContext {
    @Getter
    private final Scope scope;
    @Getter
    private final Stream stream;
    @Getter
    private final long requestId;

//    @GuardedBy("$lock")
//    private final Map<String, VersionedMetadata<?>> map = new HashMap<>();

    OperationContextImpl(Scope scope, Stream stream, long requestId) {
        this.scope = scope;
        this.stream = stream;
        this.requestId = requestId;
    }
    
//    @Synchronized
//    @Override
//    public <T> void load(String tableName, String key, VersionedMetadata<T> value) {
//        if (value != null) {
//            map.put(tableName + key, value);
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    @Synchronized
//    @Override
//    public <T> VersionedMetadata<T> get(String tableName, String key, Class<T> tClass) {
//        VersionedMetadata<?> versionedMetadata = map.get(tableName + key);
//        assert versionedMetadata == null || versionedMetadata.getObject().getClass().isAssignableFrom(tClass);
//        return (VersionedMetadata<T>) versionedMetadata;
//    }
//
//    @Synchronized
//    @Override
//    public void unload(String tableName, String key) {
//        map.remove(tableName + key);
//    }
//
//    @Synchronized
//    @Override
//    public void unloadAll() {
//        map.clear();
//    }
}
