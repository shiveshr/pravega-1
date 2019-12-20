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
import io.pravega.client.stream.notifications.ReadersSegmentDistributionNotification;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.pravega.client.stream.notifications.ReadersSegmentDistributionNotification.PROPERTY_DISTRIBUTION_POLL_INTERVAL_SECONDS;

@Slf4j
public class ReadersSegmentDistributionNotifier extends AbstractPollingNotifier<ReadersSegmentDistributionNotification> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty(PROPERTY_DISTRIBUTION_POLL_INTERVAL_SECONDS, String.valueOf(120)));

    private final Supplier<ImmutableMap<String, Integer>> readerSegmentDistributionFunction;
    
    public ReadersSegmentDistributionNotifier(final NotificationSystem notifySystem,
                                              final StateSynchronizer<ReaderGroupState> synchronizer,
                                              final Supplier<ImmutableMap<String, Integer>> readerSegmentDistributionFunction,
                                              final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizer);
        this.readerSegmentDistributionFunction = readerSegmentDistributionFunction;
    }
    
    /**
     * Invokes the periodic processing now in the current thread.
     */
    @VisibleForTesting
    public void pollNow() {
        triggerSegmentDistributionNotification();
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<ReadersSegmentDistributionNotification> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically fetch the segment count.
        startPolling(this::triggerSegmentDistributionNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return ReadersSegmentDistributionNotification.class.getSimpleName();
    }

    private void triggerSegmentDistributionNotification() {
        ImmutableMap<String, Integer> map = readerSegmentDistributionFunction.get();
        int readers = synchronizer.getState().getOnlineReaders().size();
        int segments = synchronizer.getState().getNumberOfSegments();

        if (!map.isEmpty()) {
            ReadersSegmentDistributionNotification notification = ReadersSegmentDistributionNotification
                    .builder().imbalancedReaders(map).segmentCount(segments).readerCount(readers).build();
            notifySystem.notify(notification);
        }
    }
}
