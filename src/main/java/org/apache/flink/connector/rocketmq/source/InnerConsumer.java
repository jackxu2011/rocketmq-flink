/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.connector.rocketmq.source.reader.ConsumerRecords;

import org.apache.rocketmq.common.message.MessageQueue;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface InnerConsumer extends AutoCloseable {

    /**
     * Returns a set of message queues that are assigned to the current consumer. The assignment is
     * typically performed by a message broker and may change dynamically based on various factors
     * such as load balancing and consumer group membership.
     *
     * @return A set of message queues that are currently assigned to the consumer.
     */
    Set<MessageQueue> assignment();

    /** start inner consumer */
    void start();

    /**
     * Manually assign a list of message queues to this consumer. This interface does not allow for
     * incremental assignment and will replace the previous assignment (if there is one).
     *
     * @param messageQueues Message queues that needs to be assigned.
     */
    void assign(Collection<MessageQueue> messageQueues);

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @return list of message, can be null.
     */
    ConsumerRecords poll(Duration timeout);

    /** Manually commit consume offset saved by the system. This is a non-blocking method. */
    void commit();

    /**
     * Offset specified by batch commit
     *
     * @param offsetMap Offset specified by batch commit
     * @param persist Whether to persist to the broker
     */
    void commit(Map<MessageQueue, Long> offsetMap, boolean persist);

    /**
     * Manually commit consume offset saved by the system.
     *
     * @param messageQueues Message queues that need to submit consumer offset
     * @param persist hether to persist to the broker
     */
    void commit(final Set<MessageQueue> messageQueues, boolean persist);

    /**
     * Offset specified by batch commit, incremental, and not persist
     *
     * @param offsetMap Offset specified by batch commit
     */
    CompletableFuture<Void> commitAsync(Map<MessageQueue, Long> offsetMap);

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this method is
     * invoked for the same message queue more than once, the latest offset will be used on the next
     * {@link #poll(Duration)}.
     *
     * @param messageQueue the message queue to override the fetch offset.
     * @param offset message offset.
     */
    void seek(MessageQueue messageQueue, long offset);

    /**
     * Overrides the fetch offsets with the begin offset that the consumer will use on the next
     * poll. If this API is invoked for the same message queue more than once, the latest offset
     * will be used on the next poll(). Note that you may lose data if this API is arbitrarily used
     * in the middle of consumption.
     *
     * @param messageQueue
     */
    void seekToBegin(MessageQueue messageQueue);

    /**
     * Overrides the fetch offsets with the end offset that the consumer will use on the next poll.
     * If this API is invoked for the same message queue more than once, the latest offset will be
     * used on the next poll(). Note that you may lose data if this API is arbitrarily used in the
     * middle of consumption.
     *
     * @param messageQueue rocketmq queue to locate single queue
     */
    void seekToEnd(MessageQueue messageQueue);

    /**
     * Get consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    long committed(MessageQueue messageQueue);

    /**
     * Get consumer group previously committed offset
     *
     * @param messageQueues rocketmq queue to locate queues
     * @return offset for message queue
     */
    Map<MessageQueue, Long> committed(Collection<MessageQueue> messageQueues);

    /** Get the consumer group of the consumer. */
    String getConsumerGroup();

    /**
     * Fetch partitions(message queues) of the topic.
     *
     * @param topic topic list
     * @return key is topic, values are message queue collections
     */
    Collection<MessageQueue> partitionsFor(String topic);

    /**
     * Get the consumer group's beginning offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    long beginOffset(MessageQueue messageQueue);

    /**
     * Get the consumer group's begin offsets
     *
     * @param messageQueues rocketmq queue to locate single queue
     * @return offset for message queues
     */
    Map<MessageQueue, Long> beginOffsets(Collection<MessageQueue> messageQueues);

    /**
     * Get the consumer group's end offsets
     *
     * @param messageQueues rocketmq queue to locate single queue
     * @return offset for message queues
     */
    Map<MessageQueue, Long> endOffsets(Collection<MessageQueue> messageQueues);

    /**
     * Get the consumer group's beginning offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    long endOffset(MessageQueue messageQueue);

    /**
     * Get the consumer group's offset by timestamp
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    long offsetForTime(MessageQueue messageQueue, long timestamp);

    /**
     * Get the consumer group's offset by timestamp
     *
     * @param messageQueueWithTimeMap rocketmq queue and timestamp map
     * @return offset for message queue
     */
    Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap);

    /**
     * Suspending message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be suspended.
     */
    void pause(Collection<MessageQueue> messageQueues);

    /**
     * Resuming message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be resumed.
     */
    void resume(Collection<MessageQueue> messageQueues);

    /** interrupt poll message */
    void wakeup();
}
