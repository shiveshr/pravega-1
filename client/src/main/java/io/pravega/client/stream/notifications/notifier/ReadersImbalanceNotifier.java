/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.ReadersImbalanceNotification;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

@Slf4j
public class ReadersImbalanceNotifier extends AbstractPollingNotifier<ReadersImbalanceNotification> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.segmentNotification.poll.interval.seconds", String.valueOf(120)));
    private final Function<String, Integer> readerSegmentCountFunction;

    public ReadersImbalanceNotifier(final NotificationSystem notifySystem,
                                    final StateSynchronizer<ReaderGroupState> synchronizer,
                                    final Function<String, Integer> readerSegmentCountFunction,
                                    final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizer);
        this.readerSegmentCountFunction = readerSegmentCountFunction;
    }
    
    /**
     * Invokes the periodic processing now in the current thread.
     */
    @VisibleForTesting
    public void pollNow() {
        checkAndTriggerImbalanceNotification();
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<ReadersImbalanceNotification> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically fetch the segment count.
        startPolling(this::checkAndTriggerImbalanceNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return ReadersImbalanceNotification.class.getSimpleName();
    }

    private void checkAndTriggerImbalanceNotification() {
        synchronizer.fetchUpdates();
        Set<String> onlineReaders = synchronizer.getState().getOnlineReaders();
        int readers = synchronizer.getState().getOnlineReaders().size();
        int segments = synchronizer.getState().getNumberOfSegments();

        ImmutableMap.Builder<String, Integer> mapBuilder = ImmutableMap.builder();

        onlineReaders.forEach(readerId -> {
            int assignment = readerSegmentCountFunction.apply(readerId);
            double fairLoad = segments / readers;
            boolean isImbalanced = assignment - fairLoad > 1.0;
            if (isImbalanced) {
                mapBuilder.put(readerId, assignment);
            }
        });

        ImmutableMap<String, Integer> map = mapBuilder.build();
        if (!map.isEmpty()) {
            ReadersImbalanceNotification notification = ReadersImbalanceNotification
                    .builder().imbalancedReaders(map).segmentCount(segments).readerCount(readers).build();
            notifySystem.notify(notification);
        }
    }
}
