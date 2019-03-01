/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.TestingServerStarter;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Task test cases.
 */
@Slf4j
public abstract class RequestSweeperTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected CuratorFramework cli;

    private final String stream1 = "stream1";

    private StreamMetadataStore streamStore;

    private final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
    
    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;
    private EventStreamWriterMock<ControllerEvent> requestEventWriter;
    private SegmentHelper segmentHelperMock;
    private final RequestTracker requestTracker = new RequestTracker(true);

    abstract StreamMetadataStore getStream();

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        streamStore = getStream();

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                                        .build());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock(hostStore, connectionFactory, AuthHelper.getDisabledAuthHelper());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(), segmentHelperMock,
                executor, HOSTNAME, requestTracker);
        requestEventWriter = spy(new EventStreamWriterMock<>());
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // region createStream
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        cli.close();
        zkServer.stop();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testTaskSweeper() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);

        CompletableFuture<ControllerEvent> writeEventCalled = new CompletableFuture<>();
        CompletableFuture<Void> writeEventFuture = new CompletableFuture<>();
        doAnswer(x -> {
            writeEventCalled.complete(x.getArgument(1));
            return writeEventFuture;
        }).when(requestEventWriter).writeEvent(any());
        streamMetadataTasks.manualScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), System.currentTimeMillis(), null);
        
        writeEventCalled.join();
        // since we dont complete writeEventFuture, manual scale will not complete
        
        
        
        
        RequestSweeper requestSweeper = new RequestSweeper(streamStore, executor, streamMetadataTasks);
        requestSweeper.handleFailedProcess(deadHost).get();
        
        // verify that the event is posted for all methods of interest. 
    }
}

