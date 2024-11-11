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

package org.apache.flink.connector.rocketmq.source.enumerator.initializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.rocketmq.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * An interface for users to specify the starting / stopping offset of a {@link
 * RocketMQPartitionSplit}.
 */
@PublicEvolving
public interface OffsetsInitializer extends Serializable {

    /**
     * This method retrieves the current offsets for a collection of {@link MessageQueue}s using a
     * provided {@link MessageQueueOffsetsRetriever}.
     *
     * @param messageQueues the collection of queues for which to retrieve the offsets
     * @param offsetsRetriever the offsets retriever to use for obtaining the offsets
     * @return a mapping of {@link MessageQueue}s to their current offsets
     */
    Map<MessageQueue, Long> getMessageQueueOffsets(
            Collection<MessageQueue> messageQueues, MessageQueueOffsetsRetriever offsetsRetriever);

    /**
     * Returns the strategy for automatically resetting the offset when there is no initial offset
     * in RocketMQ or if the current offset does not exist in RocketMQ.
     *
     * @return strategy for automatically resetting the offset
     */
    OffsetResetStrategy getAutoOffsetResetStrategy();

    ConsumeFromWhere getConsumeFromWhere();

    /**
     * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
     * initial offsets of the RocketMQ message queues.
     */
    interface MessageQueueOffsetsRetriever {

        /**
         * The group id should be the set for {@link RocketMQSource } before invoking this method.
         * Otherwise, an {@code IllegalStateException} will be thrown.
         */
        Map<MessageQueue, Long> committed(Collection<MessageQueue> messageQueues);

        /** List begin offsets for the specified MessageQueues. */
        Map<MessageQueue, Long> beginOffsets(Collection<MessageQueue> messageQueues);

        /** List end offsets for the specified MessageQueues. */
        Map<MessageQueue, Long> endOffsets(Collection<MessageQueue> messageQueues);

        /** List offsets for the specified timestamp. */
        Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap);
    }

    // --------------- factory methods ---------------

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the committed offsets. An
     * exception will be thrown at runtime if there is no committed offsets.
     *
     * @return an offset initializer which initialize the offsets to the committed offsets.
     */
    static OffsetsInitializer commited() {
        return commited(OffsetResetStrategy.LATEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the committed offsets. Use
     * the given {@link OffsetResetStrategy} to initialize the offsets if the committed offsets does
     * not exist.
     *
     * @param offsetResetStrategy the offset reset strategy to use when the committed offsets do not
     *     exist.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the committed offsets.
     */
    static OffsetsInitializer commited(OffsetResetStrategy offsetResetStrategy) {
        // Because ConsumeFromWhere does have CONSUME_FROM_COMMITTED option, use
        // CONSUME_FROM_TIMESTAMP as sentinel
        return new StrategyOffsetsInitializer(
                ConsumeFromWhere.CONSUME_FROM_TIMESTAMP, offsetResetStrategy);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets in each partition so that the
     * initialized offset is the offset of the first record whose record timestamp is greater than
     * or equals the give timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the consumption.
     * @return an {@link OffsetsInitializer} which initializes the offsets based on the given
     *     timestamp.
     */
    static OffsetsInitializer timestamp(long timestamp) {
        return new TimestampOffsetsInitializer(timestamp);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     * offsets of each partition.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     *     offsets.
     */
    static OffsetsInitializer earliest() {
        return new StrategyOffsetsInitializer(
                ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, OffsetResetStrategy.EARLIEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the latest available
     * offsets of each partition.
     *
     * @return an offset initializer which initialize the offsets to the latest available offsets.
     */
    static OffsetsInitializer latest() {
        return new StrategyOffsetsInitializer(
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, OffsetResetStrategy.LATEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     *
     * @param offsets the specified offsets for each partition.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     */
    static OffsetsInitializer offsets(Map<MessageQueue, Long> offsets) {
        return new SpecifiedOffsetsInitializer(offsets, OffsetResetStrategy.EARLIEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the specified offsets. Use
     * the given {@link OffsetResetStrategy} to initialize the offsets in case the specified offset
     * is out of range.
     *
     * @param offsets the specified offsets for each partition.
     * @param offsetResetStrategy the {@link OffsetResetStrategy} to use when the specified offset
     *     is out of range.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     */
    static OffsetsInitializer offsets(
            Map<MessageQueue, Long> offsets, OffsetResetStrategy offsetResetStrategy) {
        return new SpecifiedOffsetsInitializer(offsets, offsetResetStrategy);
    }
}
