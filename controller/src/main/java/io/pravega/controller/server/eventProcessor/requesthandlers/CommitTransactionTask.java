/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.CommitEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for processing commit events in request-stream.
 */
@Slf4j
public class CommitTransactionTask implements StreamTask<CommitEvent> {
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;
    private final BlockingQueue<CommitEvent> processedEvents;

    public CommitTransactionTask(final StreamMetadataStore streamMetadataStore,
                                 final StreamMetadataTasks streamMetadataTasks,
                                 final ScheduledExecutorService executor) {
        this(streamMetadataStore, streamMetadataTasks, executor, null);
    }

    @VisibleForTesting
    public CommitTransactionTask(final StreamMetadataStore streamMetadataStore,
                                 final StreamMetadataTasks streamMetadataTasks,
                                 final ScheduledExecutorService executor,
                                 final BlockingQueue<CommitEvent> queue) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = queue;
    }

    @Override
    public CompletableFuture<Void> execute(CommitEvent event) {
        // 1. check if txn-commit-node exists.
        //      if node exists and has same epoch, process the node.
        // 2. if node doesnt exist, collect all committing txn in the epoch and create txn-commit-node.
        // 3. set state to committing. // this ensures no scale can happen even if we failover and resume from an older checkpoint.
        // 4.1 if epoch is same as active epoch, we can simply merge the transaction segments into parent segments.
        // 4.1.a loop over the transactions and commit one transaction at a time until all such transactions are committed.
        // 4.2 if epoch is behind active epoch, try creating epochTransitionNode. If we fail to create it because it exists and
        // is different from what we intend to post, that can only be if it is posted by SCALING and we cant proceed for now.
        // Reset state to ACTIVE and throw OperationNotAllowed. That will get the commit processing postponed and retried later.
        // 4.2.a create duplicate epoch segments.
        // 4.2.b merge transactions on duplicate epoch segments.
        // 4.2.c seal duplicate epoch segments.
        // 4.2.d create duplicate segments for active epoch
        // 4.2.e add completed record for txnCommitEpoch and partial record For duplicateActiveEpoch to history table.
        // 4.2.f seal active record.
        // 5. delete txn-commit-node
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Attempting to commit available transactions on epoch {} on stream {}/{}", event.getEpoch(), event.getScope(), event.getStream());

        return tryCommitTransactions(scope, stream, epoch, context, this.streamMetadataTasks.retrieveDelegationToken())
                .whenCompleteAsync((result, error) -> {
                    if (error != null) {
                        log.error("Exception while attempting to committ transaction on epoch {} on stream {}/{}", epoch, scope, stream, error);
                    } else {
                        log.debug("Successfully committed transactions on epoch {} on stream {}/{}", epoch, scope, stream);
                        if (processedEvents != null) {
                            processedEvents.offer(event);
                        }
                    }
                }, executor);
    }

    @Override
    public CompletableFuture<Void> writeBack(CommitEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    private CompletableFuture<Void> tryCommitTransactions(final String scope,
                                                  final String stream,
                                                  final int epoch,
                                                  final OperationContext context,
                                                  final String delegationToken) {
        // try creating txn commit list first. if node already exists and doesnt match the processing in the event, throw operation not allowed.
        // This will result in event being posted back in the stream and retried later. Generally if a transaction commit starts, it will come to
        // an end.. but during failover, once we have created the node, we are guaranteed that it will be only that transaction that will be getting
        // committed at that time.
        CompletableFuture<List<UUID>> txnListFuture = createAndGetCommitTxnListRecord(scope, stream, epoch, context);

        CompletableFuture<Void> commitFuture = txnListFuture
                .thenCompose(txnList -> {
                    if (txnList.isEmpty()) {
                        // reset state conditionally in case we were left with stale committing state from a previous execution
                        // that died just before updating the state back to ACTIVE.
                        return streamMetadataStore.resetStateConditionally(scope, stream, State.COMMITTING, context, executor);
                    } else {
                        // Try to set the state of the stream to COMMITTING.
                        // Once state is set to committing, we are guaranteed that this will be the only processing that can happen on the stream
                        // and we can proceed with committing outstanding transactions collected in the txnList step.
                        // If we are not able to update the state it means there is some other ongoing
                        // processing and operation not allowed will mean this processing will be postponed with txn commit node created.
                        return streamMetadataStore.setState(scope, stream, State.COMMITTING, context, executor)
                                .thenCompose(v -> getEpochRecords(scope, stream, epoch, context)
                                        .thenCompose(records -> {
                                            HistoryRecord epochRecord = records.get(0);
                                            HistoryRecord activeEpochRecord = records.get(1);
                                            if (activeEpochRecord.getEpoch() == epoch ||
                                                    activeEpochRecord.getReference() == epochRecord.getReference()) {
                                                // if transactions were created on active epoch or a duplicate of active epoch, we can commit transactions immediately
                                                return commitTransactionsOnActiveEpoch(scope, stream, epoch, epochRecord.getSegments(), txnList, context);
                                            } else {
                                                return commitTransactionsOnOldEpoch(scope, stream, epoch, txnList, context);
                                            }
                                        }));
                    }
                });

        // once all commits are done, reset state to ACTIVE and delete commit txn node.
        return Futures.toVoid(commitFuture
                .thenCompose(v -> streamMetadataStore.deleteTxnCommitList(scope, stream, context, executor))
                .thenCompose(v -> streamMetadataStore.setState(scope, stream, State.ACTIVE, context, executor)));
    }

    private CompletableFuture<List<HistoryRecord>> getEpochRecords(String scope, String stream, int epoch, OperationContext context) {
        List<CompletableFuture<HistoryRecord>> list = new ArrayList<>();
        list.add(streamMetadataStore.getEpochRecord(scope, stream, epoch, context, executor));
        list.add(streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor));
        return Futures.allOfWithResults(list);
    }

    private CompletableFuture<Void> commitTransactionsOnOldEpoch(String scope, String stream, int epoch, List<UUID> txnList, OperationContext context) {
        // first compute what the epoch transition node will look like.
        // then try to createEpochTransitionNode if it doesnt exist.
        // now retrieve epochTransitionNode. if it matches what we intended to create, we can proceed.
        // else its likely created by a scale operation (either manual or post failover recovery).
        // throw opeartion not allowed
        // if we have successfully created

    }

    private CompletableFuture<Void> commitTransactionsOnActiveEpoch(String scope, String stream, int epoch, List<Long> segments,
                                                                    List<UUID> transactionsToCommit, OperationContext context) {
        // Chain all transaction commit futures one after the other. This will ensure that order of commit
        // if honoured and is based on the order in the list.
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (UUID txnId : transactionsToCommit) {
            log.debug("Committing transaction {} on stream {}/{}", txnId, scope, stream);
            future = future.thenCompose(v -> streamMetadataTasks.notifyTxnCommit(scope, stream, segments, txnId)
                    .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, epoch, txnId, context, executor)
                            .thenAccept(done -> {
                                log.debug("transaction {} on stream {}/{} committed successfully", txnId, scope, stream);
                            })));
        }
        return future;
    }

    private CompletableFuture<List<UUID>> createAndGetCommitTxnListRecord(String scope, String stream, int epoch, OperationContext context) {
        return streamMetadataStore.getCommittingTransactionsRecord(scope, stream, context, executor)
                .thenCompose(record -> {
                    if (record == null) {
                        // no ongoing commits on transactions.
                        return createNewTxnCommitList(scope, stream, epoch, context, executor);
                    } else {
                        // check if the epoch in record matches current epoch. if not throw OperationNotAllowed
                        if (record.getEpoch() == epoch) {
                            // Note: If there are transactions that are not included in the commitList but have committing state,
                            // we can be sure they will be completed through another event for this epoch.
                            return CompletableFuture.completedFuture(record.getTransactionsToCommit());
                        } else {
                            log.debug("Postponing commit on epoch {} as transactions on different epoch {} are being committed for stream {}/{}",
                                    epoch, record.getEpoch(), scope, stream);
                            throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                                    "Transactions on different epoch are being committed");
                        }
                    }
                });
    }

    private CompletableFuture<List<UUID>> createNewTxnCommitList(String scope, String stream, int epoch,
                                                                 OperationContext context, ScheduledExecutorService executor) {
        return streamMetadataStore.getTransactionsInEpoch(scope, stream, epoch, context, executor)
                .thenApply(transactions -> transactions.entrySet().stream()
                        .filter(entry -> entry.getValue().getTxnStatus().equals(TxnStatus.COMMITTING))
                        .map(Map.Entry::getKey).collect(Collectors.toList()))
                .thenCompose(transactions -> streamMetadataStore.createTxnCommitList(scope, stream, epoch, transactions, context, executor)
                        .thenApply(x -> {
                            log.debug("Transactions {} added to commit list for epoch {} stream {}/{}", transactions, epoch, scope, stream);
                            return transactions;
                        }));
    }
}
