/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQPartitionSplit implements SourceSplit {

    public static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;
    // Indicating the split should consume from the last committed offset.
    public static final long COMMITTED_OFFSET = -3;
    // Indicating the split should consume from the earliest.
    public static final long EARLIEST_OFFSET = -2;
    // Indicating the split should consume from the latest.
    public static final long LATEST_OFFSET = -1;

    // Valid special starting offsets
    public static final Set<Long> VALID_STARTING_OFFSET_MARKERS =
            new HashSet<>(Arrays.asList(EARLIEST_OFFSET, LATEST_OFFSET, COMMITTED_OFFSET));
    public static final Set<Long> VALID_STOPPING_OFFSET_MARKERS =
            new HashSet<>(Arrays.asList(LATEST_OFFSET, COMMITTED_OFFSET, NO_STOPPING_OFFSET));

    private final MessageQueue messageQueue;
    private final long startingOffset;
    private final long stoppingOffset;

    public RocketMQPartitionSplit(MessageQueue messageQueue, long startingOffset) {
        this(messageQueue, startingOffset, NO_STOPPING_OFFSET);
    }

    public RocketMQPartitionSplit(
            MessageQueue messageQueue, long startingOffset, long stoppingOffset) {
        verifyInitialOffset(messageQueue, startingOffset, stoppingOffset);
        this.messageQueue = messageQueue;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
    }

    public String getTopic() {
        return messageQueue.getTopic();
    }

    public String getBrokerName() {
        return messageQueue.getBrokerName();
    }

    public int getQueueId() {
        return messageQueue.getQueueId();
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0
                        || stoppingOffset == LATEST_OFFSET
                        || stoppingOffset == COMMITTED_OFFSET
                ? Optional.of(stoppingOffset)
                : Optional.empty();
    }

    @Override
    public String splitId() {
        return UtilAll.getSplitId(messageQueue);
    }

    @Override
    public String toString() {
        return String.format(
                "(MessageQueue: %s, StartingOffset: %d, StoppingOffset: %d)",
                messageQueue, startingOffset, stoppingOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageQueue, startingOffset, stoppingOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RocketMQPartitionSplit)) {
            return false;
        }
        RocketMQPartitionSplit other = (RocketMQPartitionSplit) obj;
        return messageQueue.equals(other.messageQueue)
                && startingOffset == other.startingOffset
                && stoppingOffset == other.stoppingOffset;
    }

    // ------------ private methods ---------------

    private static void verifyInitialOffset(
            MessageQueue tp, Long startingOffset, long stoppingOffset) {
        if (startingOffset == null) {
            throw new FlinkRuntimeException(
                    "Cannot initialize starting offset for partition " + tp);
        }

        if (startingOffset < 0 && !VALID_STARTING_OFFSET_MARKERS.contains(startingOffset)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid starting offset %d is specified for partition %s. "
                                    + "It should either be non-negative or be one of the "
                                    + "[%d(earliest), %d(latest), %d(committed)].",
                            startingOffset, tp, LATEST_OFFSET, EARLIEST_OFFSET, COMMITTED_OFFSET));
        }

        if (stoppingOffset < 0 && !VALID_STOPPING_OFFSET_MARKERS.contains(stoppingOffset)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Illegal stopping offset %d is specified for partition %s. "
                                    + "It should either be non-negative or be one of the "
                                    + "[%d(latest), %d(committed), %d(Long.MIN_VALUE, no_stopping_offset)].",
                            stoppingOffset,
                            tp,
                            LATEST_OFFSET,
                            COMMITTED_OFFSET,
                            NO_STOPPING_OFFSET));
        }
    }
}
