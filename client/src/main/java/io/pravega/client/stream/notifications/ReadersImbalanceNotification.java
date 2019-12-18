/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Class to represent a reader imbalance notification. 
 * This contains a map of readers to assigned segment count.
 * It also contains overall segment count and reader count for readers and segments managed by the readergroup.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
public class ReadersImbalanceNotification extends Notification {
    private final ImmutableMap<String, Integer> imbalancedReaders;
    private final int segmentCount;
    private final int readerCount;
}
