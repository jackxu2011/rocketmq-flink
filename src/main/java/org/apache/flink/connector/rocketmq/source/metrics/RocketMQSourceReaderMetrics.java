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

package org.apache.flink.connector.rocketmq.source.metrics;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@PublicEvolving
public class RocketMQSourceReaderMetrics {

    public static final String ROCKETMQ_SOURCE_READER_METRIC_GROUP = "RocketmqSourceReader";
    public static final String TOPIC_GROUP = "topic";
    public static final String QUEUE_GROUP = "queue";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String COMMITTED_OFFSET_METRIC_GAUGE = "committedOffset";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";
    public static final String ROCKETMQ_CONSUMER_METRIC_GROUP = "RocketMQConsumer";
    public static final String CONSUMER_FETCH_MANAGER_GROUP = "consumer-fetch-manager-metrics";
    public static final String BYTES_CONSUMED_TOTAL = "bytes-consumed-total";
    public static final String RECORDS_LAG = "records-lag";
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceReaderMetrics.class);

    public static final long INITIAL_OFFSET = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    // Metric group for registering rocketMQ specific metrics
    private final MetricGroup rocketMQSourceReaderMetricGroup;

    // Successful / Failed commits counters
    private final Counter commitsSucceeded;
    private final Counter commitsFailed;

    // Map for tracking current consuming / committing offsets
    private final Map<MessageQueue, Offset> offsets = new HashMap<>();

    public RocketMQSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.rocketMQSourceReaderMetricGroup =
                sourceReaderMetricGroup.addGroup(ROCKETMQ_SOURCE_READER_METRIC_GROUP);
        this.commitsSucceeded =
                rocketMQSourceReaderMetricGroup.counter(COMMITS_SUCCEEDED_METRIC_COUNTER);
        this.commitsFailed = rocketMQSourceReaderMetricGroup.counter(COMMITS_FAILED_METRIC_COUNTER);
    }

    /**
     * Register metric groups for the given {@link MessageQueue}.
     *
     * @param tp Registering topic partition
     */
    public void registerMessageQueue(MessageQueue tp) {
        offsets.put(tp, Offset.newInitOffset());
        registerOffsetMetricsForMessageQueue(tp);
    }

    /**
     * Update current consuming offset of the given {@link MessageQueue}.
     *
     * @param tp Updating topic partition
     * @param offset Current consuming offset
     */
    public void recordCurrentOffset(MessageQueue tp, long offset) {
        checkMessageQueueTracked(tp);
        offsets.get(tp).currentOffset = offset;
    }

    /**
     * Update the latest committed offset of the given {@link MessageQueue}.
     *
     * @param tp Updating topic partition
     * @param offset Committing offset
     */
    public void recordCommittedOffset(MessageQueue tp, long offset) {
        checkMessageQueueTracked(tp);
        offsets.get(tp).committedOffset = offset;
    }

    /** Mark a successful commit. */
    public void recordSucceededCommit() {
        commitsSucceeded.inc();
    }

    /** Mark a failure commit. */
    public void recordFailedCommit() {
        commitsFailed.inc();
    }

    // -------- Helper functions --------
    private void registerOffsetMetricsForMessageQueue(MessageQueue messageQueue) {
        final MetricGroup messageQueueGroup =
                this.rocketMQSourceReaderMetricGroup
                        .addGroup(TOPIC_GROUP, messageQueue.getTopic())
                        .addGroup(QUEUE_GROUP, String.valueOf(messageQueue.getQueueId()));
        messageQueueGroup.gauge(
                CURRENT_OFFSET_METRIC_GAUGE,
                () -> offsets.getOrDefault(messageQueue, Offset.newInitOffset()).currentOffset);
        messageQueueGroup.gauge(
                COMMITTED_OFFSET_METRIC_GAUGE,
                () -> offsets.getOrDefault(messageQueue, Offset.newInitOffset()).committedOffset);
    }

    private void checkMessageQueueTracked(MessageQueue tp) {
        if (!offsets.containsKey(tp)) {
            throw new IllegalArgumentException(
                    String.format("TopicPartition %s is not tracked", tp));
        }
    }

    private static class Offset {
        static Offset newInitOffset() {
            return new Offset(INITIAL_OFFSET, INITIAL_OFFSET);
        }

        long currentOffset;
        long committedOffset;

        Offset(long currentOffset, long committedOffset) {
            this.currentOffset = currentOffset;
            this.committedOffset = committedOffset;
        }
    }
}
