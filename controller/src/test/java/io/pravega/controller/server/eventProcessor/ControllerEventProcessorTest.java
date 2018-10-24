/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    private ScheduledExecutorService executor;
    private StreamMetadataStore streamStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private CuratorFramework zkClient;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newScheduledThreadPool(10);

        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, TaskStoreFactory.createInMemoryStore(executor),
                segmentHelperMock, executor, "1", connectionFactory, AuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, segmentHelperMock,
                executor, "host", connectionFactory, AuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(), "abortStream",
                new EventStreamWriterMock<>());

        // region createStream
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, STREAM, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 10000)
    public void testCommitEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, txnData.getEpoch())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
    }

    @Test(timeout = 10000)
    public void testMultipleTransactionsSuccess() {
        // 1. commit request for an older epoch
        // this should be ignored
        // 2. multiple transactions in committing state
        // first event should commit all of them
        // subsequent events should be no op
        // 3. commit request for future epoch
        // this should be ignored as there is nothing in the epoch to commit
        List<VersionedTransactionData> txnDataList = createAndCommitTransactions(3);
        int epoch = txnDataList.get(0).getEpoch();
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch - 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch + 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch))));
    }

    @Test(timeout = 10000)
    public void testTransactionOutstandingCommit() {
        // keep a committxnlist in the store
        // same epoch --> commit txn list should be cleared first
        //     subsequent events should complete remainder txns that are in committing state
        // lower epoch --> rolling transaction
        // higher epoch --> no transactions can exist on higher epoch. nothing to do. ignore.
        List<VersionedTransactionData> txnDataList1 = createAndCommitTransactions(3);
        int epoch = txnDataList1.get(0).getEpoch();
        streamStore.startCommitTransactions(SCOPE, STREAM, epoch, null, executor).join();

        List<VersionedTransactionData> txnDataList2 = createAndCommitTransactions(3);

        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, executor);

        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch - 1)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch + 1)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList1) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);
        }

        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }
    }

    @Test(timeout = 30000)
    public void testCommitAndStreamProcessorFairness() {
        List<VersionedTransactionData> txnDataList1 = createAndCommitTransactions(3);
        int epoch = txnDataList1.get(0).getEpoch();
        streamStore.startCommitTransactions(SCOPE, STREAM, epoch, null, executor).join();

        EventStreamWriterMock<ControllerEvent> requestEventWriter = new EventStreamWriterMock<>();
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                null, null, null, null, streamStore, executor);

        // set some processor name so that the processing gets postponed
        streamStore.createWaitingRequestIfAbsent(SCOPE, STREAM, "test", null, executor).join();
        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStore.deleteWaitingRequestConditionally(SCOPE, STREAM, "test1", null, executor).join();
        assertEquals("test", streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        // now remove the barrier but change the state so that processing can not happen.
        streamStore.deleteWaitingRequestConditionally(SCOPE, STREAM, "test", null, executor).join();
        assertNull(streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());
        streamStore.setState(SCOPE, STREAM, State.SCALING, null, executor).join();

        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        assertEquals(commitEventProcessor.getProcessorName(), streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // verify that we are able to process if the waiting processor name is same as ours.
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();

        // verify that waiting processor is cleaned up.
        assertNull(streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        // now set the state to COMMITTING_TXN and try the same with scaling
        streamStore.setState(SCOPE, STREAM, State.COMMITTING_TXN, null, executor).join();

        // verify that event that does not try to use `processor.withCompletion` runs without contention
        assertTrue(Futures.await(streamRequestHandler.processEvent(new AutoScaleEvent(SCOPE, STREAM, 0L, AutoScaleEvent.UP, 0L, 2, true))));

        // now same event's processing in face of a barrier should get postponed
        streamStore.createWaitingRequestIfAbsent(SCOPE, STREAM, commitEventProcessor.getProcessorName(), null, executor).join();
        assertTrue(Futures.await(streamRequestHandler.processEvent(new AutoScaleEvent(SCOPE, STREAM, 0L, AutoScaleEvent.UP, 0L, 2, true))));

        AssertExtensions.assertThrows("Operation should be disallowed", streamRequestHandler.processEvent(
                new ScaleOpEvent(SCOPE, STREAM, Collections.emptyList(), Collections.emptyList(), false, 0L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        assertEquals(commitEventProcessor.getProcessorName(), streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());
    }

    private List<VersionedTransactionData> createAndCommitTransactions(int count) {
        List<VersionedTransactionData> retVal = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
            VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                    null, executor).join();
            Assert.assertNotNull(txnData);
            checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

            streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), null, executor).join();
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

            retVal.add(txnData);
        }
        return retVal;
    }

    @Test(timeout = 10000)
    public void testAbortEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), false, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTING);

        AbortRequestHandler abortRequestHandler = new AbortRequestHandler(streamStore, streamMetadataTasks, executor);
        abortRequestHandler.processEvent(new AbortEvent(SCOPE, STREAM, txnData.getEpoch(), txnData.getId())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTED);
    }

    private void checkTransactionState(String scope, String stream, UUID txnId, TxnStatus expectedStatus) {
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnId, null, executor).join();
        assertEquals(expectedStatus, txnStatus);
    }
}
