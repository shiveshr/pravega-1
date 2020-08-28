/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.metrics;

import com.google.common.base.Preconditions;
import io.pravega.shared.metrics.OpStatsLogger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.shared.MetricsNames.*;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.shared.MetricsTags.transactionTags;

/**
 * Class to encapsulate the logic to report Controller service metrics for Transactions.
 */
public final class TransactionMetrics extends AbstractControllerMetrics {

    private static final AtomicReference<TransactionMetrics> INSTANCE = new AtomicReference<>();

    private final OpStatsLogger createTransactionLatency;
    private final OpStatsLogger createTransactionSegmentsLatency;
    private final OpStatsLogger createTransactionMetadataLatency;
    private final OpStatsLogger commitTransactionMetadataUpdateLatency;
    private final OpStatsLogger commitTransactionWriteEventLatency;
    private final OpStatsLogger identifyTransactionsToCommitLatency;
    private final OpStatsLogger commitEventProcessingLatency;
    private final OpStatsLogger commitTransactionLatency;
    private final OpStatsLogger commitTransactionSegmentsLatency;
    private final OpStatsLogger committingTransactionLatency;
    private final OpStatsLogger abortTransactionLatency;
    private final OpStatsLogger abortTransactionSegmentsLatency;
    private final OpStatsLogger abortingTransactionLatency;

    private TransactionMetrics() {
        createTransactionLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_LATENCY);
        createTransactionSegmentsLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_SEGMENTS_LATENCY);
        commitTransactionLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_LATENCY);
        commitTransactionSegmentsLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_SEGMENTS_LATENCY);
        committingTransactionLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_LATENCY);
        abortTransactionLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_LATENCY);
        abortTransactionSegmentsLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_SEGMENTS_LATENCY);
        abortingTransactionLatency = STATS_LOGGER.createStats(ABORTING_TRANSACTION_LATENCY);
        createTransactionMetadataLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_METADATA_LATENCY);
        commitTransactionMetadataUpdateLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_METADATA_LATENCY);
        commitTransactionWriteEventLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_WRITE_EVENT_LATENCY);
        identifyTransactionsToCommitLatency = STATS_LOGGER.createStats(IDENTIFY_TRANSACTION_TO_COMMIT_LATENCY);
        commitEventProcessingLatency = STATS_LOGGER.createStats(COMMIT_EVENT_PROCESSING_LATENCY);
    }

    /**
     * Mandatory call to initialize the singleton object.
     */
    public static synchronized void initialize() {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new TransactionMetrics());
        }
    }

    /**
     * Get the singleton {@link TransactionMetrics} instance. It is mandatory to call initialize before invoking this method.
     *
     * @return StreamMetrics instance.
     */
    public static TransactionMetrics getInstance() {
        Preconditions.checkState(INSTANCE.get() != null, "You need call initialize before using this class.");
        return INSTANCE.get();
    }

    /**
     * This method increments the global and Stream-related counters of created Transactions and reports the latency of
     * the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the create Transaction operation.
     */
    public void createTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION, 1, streamTags(scope, streamName));
        createTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular create Transaction.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionSegments(Duration latency) {
        createTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of failed Transaction create operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     */
    public void createTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method accounts for the time taken for a client to set a Transaction to COMMITTING state.
     *
     * @param latency    Latency of the abort Transaction operation.
     */
    public void committingTransaction(Duration latency) {
        committingTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of committed Transactions and reports the latency
     * of the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the commit Transaction operation.
     */
    public void commitTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(COMMIT_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION, 1, streamTags(scope, streamName));
        commitTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular commit Transaction.
     *
     * @param latency      Time elapsed to merge the segments related to the committed transaction.
     */
    public void commitTransactionSegments(Duration latency) {
        commitTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed commit operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param txnId      Transaction id.
     */
    public void commitTransactionFailed(String scope, String streamName, String txnId) {
        commitTransactionFailed(scope, streamName);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION_FAILED, 1, transactionTags(scope, streamName, txnId));
    }

    /**
     * This method increments the global, Stream-related counters of failed commit operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     */
    public void commitTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(COMMIT_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method accounts for the time taken for a client to set a Transaction to ABORTING state.
     *
     * @param latency    Latency of the abort Transaction operation.
     */
    public void abortingTransaction(Duration latency) {
        abortingTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of aborted Transactions and reports the latency
     * of the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the abort Transaction operation.
     */
    public void abortTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ABORT_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION, 1, streamTags(scope, streamName));
        abortTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular abort Transaction.
     *
     * @param latency      Time elapsed to delete the segments related to the aborted transaction.
     */
    public void abortTransactionSegments(Duration latency) {
        abortTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed abort operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param txnId      Transaction id.
     */
    public void abortTransactionFailed(String scope, String streamName, String txnId) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ABORT_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1, transactionTags(scope, streamName, txnId));
    }

    /**
     * This method reports the current number of open Transactions for a Stream.
     *
     * @param scope                 Scope.
     * @param streamName            Name of the Stream.
     * @param ongoingTransactions   Number of open Transactions in the Stream.
     */
    public static void reportOpenTransactions(String scope, String streamName, int ongoingTransactions) {
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, ongoingTransactions, streamTags(scope, streamName));
    }

    public void createTransactionInStore(Duration latency) {
        createTransactionMetadataLatency.reportSuccessValue(latency.toMillis());
    }

    public void markTransactionAsCommitting(Duration latency) {
        commitTransactionMetadataUpdateLatency.reportSuccessValue(latency.toMillis());
    }

    public void writeCommitEvent(Duration latency) {
        commitTransactionWriteEventLatency.reportSuccessValue(latency.toMillis());
    }

    public void commitTransactionIdentification(Duration latency) {
        identifyTransactionsToCommitLatency.reportSuccessValue(latency.toMillis());
    }

    public void commitEventProcessingTime(Duration latency) {
        commitEventProcessingLatency.reportSuccessValue(latency.toMillis());
    }

    public static synchronized void reset() {
        if (INSTANCE.get() != null) {
            INSTANCE.get().createTransactionLatency.close();
            INSTANCE.get().createTransactionSegmentsLatency.close();
            INSTANCE.get().commitTransactionLatency.close();
            INSTANCE.get().commitTransactionSegmentsLatency.close();
            INSTANCE.get().committingTransactionLatency.close();
            INSTANCE.get().abortTransactionLatency.close();
            INSTANCE.get().abortTransactionSegmentsLatency.close();
            INSTANCE.get().abortingTransactionLatency.close();
            INSTANCE.set(null);
        }
    }
}
