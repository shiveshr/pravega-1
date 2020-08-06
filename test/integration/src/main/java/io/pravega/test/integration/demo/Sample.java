package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Sample {
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);

    public static void main(String[] args) throws InterruptedException {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), EXECUTOR);

        StreamManager sm = new StreamManagerImpl(clientConfig);
        String scope = "scope";
        sm.createScope(scope);
        String stream = "stream";
        sm.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.byEventRate(100, 2, 10)).build());

        // start writer

        // read pattern
        ReaderGroupManager rgm = ReaderGroupManager.withScope(scope, clientConfig);
        StreamImpl streamObj = new StreamImpl(scope, stream);
        ReaderGroupConfig cfg = ReaderGroupConfig.builder().stream(streamObj).build();
        String rgName = "grpName";
        rgm.createReaderGroup(rgName, cfg);
        ReaderGroup rg = rgm.getReaderGroup(rgName);
        AtomicBoolean run = new AtomicBoolean(true);

        periodicRolloverAndTruncation(controller, scope, stream, streamObj, rg, run);
        processEvents(clientConfig, scope, rgName, run);
        
        Thread.sleep(10000000L);
        run.set(false);
    }

    private static void periodicRolloverAndTruncation(ControllerImpl controller, String scope, String stream, StreamImpl streamObj, ReaderGroup rg, AtomicBoolean run) throws InterruptedException {
        AtomicReference<StreamCut> truncationPoint = new AtomicReference<>();
        truncationPoint.set(rolloverAndGetTruncationPoint(controller, scope, stream, streamObj, rg));

        Futures.loop (run::get, () -> Futures.delayedFuture(() -> {
            boolean isAhead = rg.getStreamCuts().get(streamObj).asImpl()
                                .getPositions().entrySet().stream().allMatch(x -> {
                        Map<Segment, Long> positions = truncationPoint.get().asImpl().getPositions();
                        if (positions.containsKey(x.getKey())) {
                            return positions.get(x.getKey()) > x.getValue();
                        } else {
                            return positions.entrySet().stream().allMatch(y -> x.getKey().getSegmentId() > y.getKey().getSegmentId());
                        }
                    });

            if (isAhead) {
                return controller.truncateStream(scope, stream, truncationPoint.get())
                        .thenAccept(v -> truncationPoint.set(rolloverAndGetTruncationPoint(controller, scope, stream, streamObj, rg)));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }, Duration.ofSeconds(10).toMillis(), EXECUTOR), EXECUTOR);
    }

    private static StreamCut rolloverAndGetTruncationPoint(ControllerImpl controller, String scope, String stream, StreamImpl streamObj, ReaderGroup rg) {
        // this is a new method that i had to add to controller client because it returned an opaque StreamSegments object from 
        // getCurrentSegments which did not expose the segments with their ranges, which is required to create identical 
        // replacement ranges.
        List<SegmentWithRange> activeSegments = controller.getCurrentSegmentsWithRange(scope, stream).join();

        List<Long> segmentsToSeal = new ArrayList<>();
        Map<Double, Double> newRanges = new HashMap<>();
        activeSegments.forEach(x -> {
            segmentsToSeal.add(x.getSegment().getSegmentId());
            newRanges.put(x.getRange().getLow(), x.getRange().getHigh());
        });

        controller.scaleStream(streamObj, segmentsToSeal, newRanges, EXECUTOR).getFuture().join();

        // get the segments post scale and create a stream cut from them.
        activeSegments = controller.getCurrentSegmentsWithRange(scope, stream).join();
        Map<Segment, Long> map = activeSegments.stream().collect(Collectors.toMap(SegmentWithRange::getSegment, x -> 0L));

        return new StreamCutImpl(streamObj, map);
    }

    private static void processEvents(ClientConfig clientConfig, String scope, String rgName, AtomicBoolean continueReading) {
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamReader<String> reader = clientFactory.createReader("reader", rgName, new JavaSerializer<>(),
                ReaderConfig.builder().build());

        Futures.loop(continueReading::get, () -> {
            return CompletableFuture.runAsync(() -> {
                EventRead<String> event = reader.readNextEvent(1000L);
                // process event
                // ...
            }, EXECUTOR);
        }, EXECUTOR);
    }
}
