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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.InnerConsumerImpl;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * A {@link SplitReader} implementation that reads records from RocketMQ partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 */
@Internal
public class RocketMQSplitReader<T> implements SplitReader<MessageView, RocketMQPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSplitReader.class);

    private final Duration POLL_TIMEOUT;
    private final InnerConsumer consumer;
    private final Map<MessageQueue, Long> stoppingOffsets = new HashMap<>();
    private final Configuration configuration;

    private final RocketMQSourceReaderMetrics rocketmqSourceReaderMetrics;
    private volatile boolean wakeup = false;

    // Tracking empty splits that has not been added to finished splits in fetch()
    private final Set<String> emptySplits = new HashSet<>();

    public RocketMQSplitReader(
            Configuration configuration, RocketMQSourceReaderMetrics rocketmqSourceReaderMetrics) {

        this.configuration = configuration;
        POLL_TIMEOUT =
                Duration.ofMillis(this.configuration.get(RocketMQSourceOptions.POLL_TIMEOUT));
        this.consumer = new InnerConsumerImpl(configuration);
        this.consumer.start();
        this.rocketmqSourceReaderMetrics = rocketmqSourceReaderMetrics;
    }

    @Override
    public RecordsWithSplitIds<MessageView> fetch() {
        wakeup = false;
        ConsumerRecords records;
        try {
            records = consumer.poll(POLL_TIMEOUT);
        } catch (IllegalStateException e) {
            // IllegalStateException will be thrown if the consumer is not assigned any partitions.
            // This happens if all assigned partitions are invalid or empty (starting offset >=
            // stopping offset). We just mark empty partitions as finished and return an empty
            // record container, and this consumer will be closed by SplitFetcherManager.
            RocketMQSplitRecords recordsBySplits =
                    new RocketMQSplitRecords(ConsumerRecords.empty(), rocketmqSourceReaderMetrics);
            markEmptySplitsAsFinished(recordsBySplits);
            return recordsBySplits;
        }
        RocketMQSplitRecords recordsBySplits =
                new RocketMQSplitRecords(records, rocketmqSourceReaderMetrics);

        List<MessageQueue> finishedPartitions = new ArrayList<>();
        for (MessageQueue partition : consumer.assignment()) {
            long stoppingOffset = getStoppingOffset(partition);
            long consumerPosition = consumer.position(partition);
            // Stop fetching when the consumer's position reaches the stoppingOffset.
            // Control messages may follow the last record; therefore, using the last record's
            // offset as a stopping condition could result in indefinite blocking.
            if (consumerPosition >= stoppingOffset) {
                LOG.debug(
                        "Position of {}: {}, has reached stopping offset: {}",
                        partition,
                        consumerPosition,
                        stoppingOffset);
                recordsBySplits.setPartitionStoppingOffset(partition, stoppingOffset);
                finishSplitAtRecord(
                        partition,
                        stoppingOffset,
                        consumerPosition,
                        finishedPartitions,
                        recordsBySplits);
            }
        }

        // Only track non-empty partition's record lag if it never appears before
        //        records
        //                .partitions()
        //                .forEach(
        //                        trackTp -> {
        //                            rocketmqSourceReaderMetrics.maybeAddRecordsLagMetric(consumer,
        // trackTp);
        //                        });

        markEmptySplitsAsFinished(recordsBySplits);

        // Unassign the partitions that has finished.
        if (!finishedPartitions.isEmpty()) {
            //
            // finishedPartitions.forEach(rocketmqSourceReaderMetrics::removeRecordsLagMetric);
            unassignPartitions(finishedPartitions);
        }

        // Update numBytesIn
        //        rocketmqSourceReaderMetrics.updateNumBytesInCounter();

        return recordsBySplits;
    }

    private void markEmptySplitsAsFinished(RocketMQSplitRecords recordsBySplits) {
        // Some splits are discovered as empty when handling split additions. These splits should be
        // added to finished splits to clean up states in split fetcher and source reader.
        if (!emptySplits.isEmpty()) {
            recordsBySplits.finishedSplits.addAll(emptySplits);
            emptySplits.clear();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQPartitionSplit> splitsChange) {
        // Current not support assign addition splits.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        ConcurrentMap<MessageQueue, Tuple2<Long, Long>> newOffsetTable = new ConcurrentHashMap<>();

        // Set up the stopping timestamps.
        splitsChange
                .splits()
                .forEach(
                        split -> {
                            MessageQueue messageQueue = split.getMessageQueue();
                            newOffsetTable.put(
                                    messageQueue,
                                    new Tuple2<>(
                                            split.getStartingOffset(), split.getStoppingOffset()));
                            rocketmqSourceReaderMetrics.registerNewMessageQueue(messageQueue);
                        });

        // todo: log message queue change

        // It will replace the previous assignment
        Set<MessageQueue> incrementalSplits = newOffsetTable.keySet();
        consumer.assign(incrementalSplits);

        // set offset to consumer
        for (Map.Entry<MessageQueue, Tuple2<Long, Long>> entry : newOffsetTable.entrySet()) {
            MessageQueue messageQueue = entry.getKey();
            Long startingOffset = entry.getValue().f0;
            try {
                consumer.seek(messageQueue, startingOffset);
            } catch (Exception e) {
                String info =
                        String.format(
                                "messageQueue:%s, seek to starting offset:%s",
                                messageQueue, startingOffset);
                throw new FlinkRuntimeException(info, e);
            }
            if (entry.getValue().f1 != RocketMQPartitionSplit.NO_STOPPING_OFFSET) {
                stoppingOffsets.put(messageQueue, entry.getValue().f1);
            }
        }

        removeEmptySplits();
    }

    @Override
    public void wakeUp() {
        LOG.debug("Wake up the split reader in case the fetcher thread is blocking in fetch().");
        wakeup = true;
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.error("close consumer error", e);
        }
    }

    private void removeEmptySplits() {
        List<MessageQueue> emptyPartitions = new ArrayList<>();
        // If none of the partitions have any records,
        for (MessageQueue partition : consumer.assignment()) {
            if (consumer.position(partition) >= getStoppingOffset(partition)) {
                emptyPartitions.add(partition);
            }
        }
        if (!emptyPartitions.isEmpty()) {
            LOG.debug(
                    "These assigning splits are empty and will be marked as finished in later fetch: {}",
                    emptyPartitions);
            // Add empty partitions to empty split set for later cleanup in fetch()
            emptySplits.addAll(
                    emptyPartitions.stream().map(UtilAll::getSplitId).collect(Collectors.toSet()));
            // Un-assign partitions from Kafka consumer
            unassignPartitions(emptyPartitions);
        }
    }

    public void notifyCheckpointComplete(Map<MessageQueue, Long> offsetsToCommit) {
        if (offsetsToCommit != null) {
            for (Map.Entry<MessageQueue, Long> entry : offsetsToCommit.entrySet()) {
                consumer.commitOffset(entry.getKey(), entry.getValue());
            }
        }
    }

    private void unassignPartitions(Collection<MessageQueue> partitionsToUnassign) {
        Collection<MessageQueue> newAssignment = new HashSet<>(consumer.assignment());
        newAssignment.removeAll(partitionsToUnassign);
        consumer.assign(newAssignment);
    }

    private void finishSplitAtRecord(
            MessageQueue partition,
            long stoppingOffset,
            long currentOffset,
            List<MessageQueue> finishedPartitions,
            RocketMQSplitRecords recordsBySplits) {
        LOG.debug(
                "{} has reached stopping offset {}, current offset is {}",
                partition,
                stoppingOffset,
                currentOffset);
        finishedPartitions.add(partition);
        recordsBySplits.addFinishedSplit(UtilAll.getSplitId(partition));
    }

    private long getStoppingOffset(MessageQueue partition) {
        return stoppingOffsets.getOrDefault(partition, Long.MAX_VALUE);
    }

    // ---------------- private helper class ------------------------

    private static class RocketMQSplitRecords implements RecordsWithSplitIds<MessageView> {

        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<MessageQueue, Long> stoppingOffsets = new HashMap<>();
        private final ConsumerRecords consumerRecords;
        private final RocketMQSourceReaderMetrics metrics;
        private final Iterator<MessageQueue> splitIterator;
        private Iterator<MessageView> recordIterator;
        private MessageQueue currentTopicPartition;
        private Long currentSplitStoppingOffset;

        public RocketMQSplitRecords(
                ConsumerRecords consumerRecords, RocketMQSourceReaderMetrics metrics) {
            this.consumerRecords = consumerRecords;
            this.splitIterator = consumerRecords.partitions().iterator();
            this.metrics = metrics;
        }

        private void setPartitionStoppingOffset(MessageQueue partition, long stoppingOffset) {
            stoppingOffsets.put(partition, stoppingOffset);
        }

        private void addFinishedSplit(String splitId) {
            this.finishedSplits.add(splitId);
        }

        /**
         * Moves to the next split. This method is also called initially to move to the first split.
         * Returns null, if no splits are left.
         */
        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                currentTopicPartition = splitIterator.next();
                recordIterator = consumerRecords.records(currentTopicPartition).iterator();
                currentSplitStoppingOffset =
                        stoppingOffsets.getOrDefault(currentTopicPartition, Long.MAX_VALUE);

                return UtilAll.getSplitId(currentTopicPartition);
            } else {
                currentTopicPartition = null;
                recordIterator = null;
                currentSplitStoppingOffset = null;
                return null;
            }
        }

        /**
         * Gets the next record from the current split. Returns null if no more records are left in
         * this split.
         */
        @Nullable
        @Override
        public MessageView nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentTopicPartition,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                final MessageView record = recordIterator.next();
                // Only emit records before stopping offset
                if (record.getQueueOffset() < currentSplitStoppingOffset) {
                    //                    metrics.recordCurrentOffset(currentTopicPartition,
                    // record.offset());
                    return record;
                }
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
