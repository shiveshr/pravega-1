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

import com.google.common.base.Strings;
import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;

@Slf4j
public class PravegaTableScope implements Scope {
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "Table" + SEPARATOR + "streamsInScope" + SEPARATOR + "%s" + SEPARATOR + "%s";
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    private final AtomicReference<String> streamsInScopeRef;
    
    PravegaTableScope(final String scopeName, PravegaTablesStoreHelper storeHelper, Executor executor) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.streamsInScopeRef = new AtomicReference<>(null);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        // add entry to scopes table followed by creating scope specific table
        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntry(
                NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName, newId()),
                DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE)
                                 .thenCompose(v -> {
                                     log.debug("table created {}/{}", NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE);
                                     return storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName, newId());
                                 }))
                      .thenCompose(v -> getStreamsInScopeTableName())
                      .thenCompose(tableName -> storeHelper.createTable(scopeName, tableName)
                      .thenAccept(v -> log.debug("table created {}/{}", scopeName, tableName)));
    }

    private byte[] newId() {
        byte[] b = new byte[2 * Long.BYTES];
        BitConverter.writeUUID(b, 0, UUID.randomUUID());
        return b;
    }

    CompletableFuture<String> getStreamsInScopeTableName() {
        String name = streamsInScopeRef.get();
        if (Strings.isNullOrEmpty(name)) {
            return storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName)
                              .thenCompose(entry -> {
                                  UUID id = BitConverter.readUUID(entry.getData(), 0);
                                  String streamsInScopeTable = String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, scopeName, id);
                                  streamsInScopeRef.compareAndSet(null, streamsInScopeTable);
                                  return getStreamsInScopeTableName();
                              });
        } else {
            return CompletableFuture.completedFuture(name);
        }
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.deleteTable(scopeName, tableName, true)
                                                     .thenAccept(v -> log.debug("table deleted {}/{}", scopeName, tableName)))
                .thenCompose(deleted -> storeHelper.removeEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor) {
        List<String> taken = new ArrayList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return getStreamsInScopeTableName()
            .thenCompose(entry -> storeHelper.getKeysPaginated(scopeName, entry, 
                        Unpooled.wrappedBuffer(Base64.getDecoder().decode(token.get())), limit)
                .thenApply(result -> {
                    if (result.getValue().isEmpty()) {
                        canContinue.set(false);
                    } else {
                        taken.addAll(result.getValue());
                    }
                    token.set(Base64.getEncoder().encodeToString(result.getKey().array()));
                    return new ImmutablePair<>(taken, token.get());
                }));
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        List<String> result = new ArrayList<>();
        return getStreamsInScopeTableName()
            .thenCompose(tableName -> storeHelper.getAllKeys(scopeName, tableName).collectRemaining(result::add)
                .thenApply(v -> result));
    }

    @Override
    public void refresh() {
        streamsInScopeRef.set(null);
    }

    CompletableFuture<Void> addStreamToScope(String stream) {
        return getStreamsInScopeTableName()
            .thenCompose(tableName -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(scopeName, tableName, stream, newId())));
    }
    
    CompletableFuture<Void> removeStreamFromScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(scopeName, tableName, stream)));
    }
}
