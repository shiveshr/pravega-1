package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
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

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        AtomicBoolean run = new AtomicBoolean(true);
        
        // read pattern
        ReaderGroupManager rgm = ReaderGroupManager.withScope(scope, clientConfig);
        StreamImpl streamObj = new StreamImpl(scope, stream);
        ReaderGroupConfig cfg = ReaderGroupConfig.builder().stream(streamObj).build();
        String rgName = "grpName";
        rgm.createReaderGroup(rgName, cfg);
        ReaderGroup rg = rgm.getReaderGroup(rgName);

        startBackgroundRolloverAndTruncation(controller, scope, stream, streamObj, Collections.singletonList(rg), run);

        // start writer
        startWriter(stream, clientFactory, run);

        // start reader
        startReader(clientFactory, rgName, run);

        // let traffic be generated for 20 minutes after which we terminate. 
        Thread.sleep(Duration.ofMinutes(20).toMillis());
        run.set(false);
    }

    private static void startWriter(String stream, EventStreamClientFactory clientFactory, AtomicBoolean run) {
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(stream, new JavaSerializer<>(), EventWriterConfig.builder().build());
        Futures.loop(run::get, () -> writer.writeEvent("event"), EXECUTOR);
    }

    private static void startReader(EventStreamClientFactory clientFactory, String rgName, AtomicBoolean continueReading) {
        EventStreamReader<String> reader = clientFactory.createReader("reader", rgName, new JavaSerializer<>(),
                ReaderConfig.builder().build());

        Futures.loop(continueReading::get, () -> {
            return CompletableFuture.runAsync(() -> {
                EventRead<String> event = reader.readNextEvent(1000L);
                // TODO: process event
                // ...
            }, EXECUTOR);
        }, EXECUTOR);
    }

    private static void startBackgroundRolloverAndTruncation(ControllerImpl controller, String scope, String stream, StreamImpl streamObj,
                                                             List<ReaderGroup> rg, AtomicBoolean run) {
        AtomicReference<StreamCut> nextTruncationPoint = new AtomicReference<>();
        rolloverAndGetTruncationPoint(controller, scope, stream, streamObj)
                .thenCompose(tp -> {
                    nextTruncationPoint.set(tp);
                    // every 10 seconds, we check whether all readergroups are ahead of next truncation point.  
                    return Futures.loop(run::get, () -> Futures.delayedFuture(() -> {
                        return Futures.allOfWithResults(rg.stream().map(m -> m.generateStreamCuts(EXECUTOR).thenApply(streamCut -> {
                            return isRGAheadOfTruncationPoint(streamObj, nextTruncationPoint, streamCut);
                        })).collect(Collectors.toList()))
                                      .thenCompose(list -> {
                                          // if all readergroups are ahead of truncation point, then we can truncate at the truncation point. 
                                          boolean allAhead = list.stream().reduce(true, (a, b) -> a && b);
                                          if (allAhead) {
                                              return controller.truncateStream(scope, stream, nextTruncationPoint.get())
                                                               // after truncating, we will rollover again and choose the next truncation point
                                                               .thenCompose(v -> rolloverAndGetTruncationPoint(controller, scope, stream, streamObj))
                                                               .thenAccept(nextTruncationPoint::set);
                                          } else {
                                              return CompletableFuture.<Void>completedFuture(null);
                                          }
                                      });
                    }, Duration.ofSeconds(10).toMillis(), EXECUTOR), EXECUTOR);
                });
    }

    private static boolean isRGAheadOfTruncationPoint(StreamImpl streamObj, AtomicReference<StreamCut> truncationPoint, Map<Stream, StreamCut> streamCut) {
        return streamCut.get(streamObj).asImpl()
                        .getPositions().entrySet().stream().allMatch(x -> {
                    Map<Segment, Long> positions = truncationPoint.get().asImpl().getPositions();
                    if (positions.containsKey(x.getKey())) {
                        return positions.get(x.getKey()) > x.getValue();
                    } else {
                        return positions.entrySet().stream().allMatch(y -> x.getKey().getSegmentId() > y.getKey().getSegmentId());
                    }
                });
    }

    private static CompletableFuture<StreamCut> rolloverAndGetTruncationPoint(ControllerImpl controller, String scope, String stream, StreamImpl streamObj) {
        // this is a new method that i had to add to controller client because it returned an opaque StreamSegments object from 
        // getCurrentSegments which did not expose the segments with their ranges, which is required to create identical 
        // replacement ranges.
        return controller.rollOver(scope, stream, EXECUTOR)
                              .thenApply(newSegments -> {
                                  // get the segments post scale and create a stream cut from them.
                                  Map<Segment, Long> map = newSegments.getSegments().stream().collect(
                                          Collectors.toMap(x -> x, x -> 0L));
                                  return new StreamCutImpl(streamObj, map);                                  
                              });
    }
}
