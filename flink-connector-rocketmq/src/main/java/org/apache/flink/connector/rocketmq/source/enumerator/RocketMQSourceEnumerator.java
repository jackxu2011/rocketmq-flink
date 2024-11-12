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

package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.RocketMQConsumer;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategyFactory;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** The enumerator class for RocketMQ source. */
@Internal
public class RocketMQSourceEnumerator
        implements SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceEnumerator.class);

    // Users can specify the starting / stopping offset initializer.
    private final AllocateStrategy allocateStrategy;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    private final OffsetsInitializer newDiscoveryOffsetsInitializer;
    private final Configuration configuration;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<RocketMQPartitionSplit> context;
    private final Boundedness boundedness;
    /** Partitions that have been assigned to readers. */
    private final Set<MessageQueue> assignedPartitions;
    /**
     * The partitions that have been discovered during initialization but not assigned to readers
     * yet.
     */
    private final Set<MessageQueue> unassignedInitialPartitions;

    private final Map<Integer, Set<RocketMQPartitionSplit>> pendingPartitionSplitAssignment;
    // Param from configuration
    private final String groupId;
    private InnerConsumer consumer;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;
    // this flag will be marked as true if initial partitions are discovered after enumerator starts
    private boolean initialDiscoveryFinished;

    public RocketMQSourceEnumerator(
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQPartitionSplit> context) {

        this(
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                configuration,
                context,
                new RocketMQSourceEnumState(Collections.emptySet(), false));
    }

    public RocketMQSourceEnumerator(
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQPartitionSplit> context,
            RocketMQSourceEnumState rocketMQSourceEnumState) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.configuration = configuration;
        this.context = context;
        this.boundedness = boundedness;

        this.assignedPartitions = new HashSet<>(rocketMQSourceEnumState.assignedPartitions());
        this.unassignedInitialPartitions =
                new HashSet<>(rocketMQSourceEnumState.unassignedInitialPartitions());
        // Support allocate splits to reader
        this.pendingPartitionSplitAssignment = new HashMap<>();
        this.partitionDiscoveryIntervalMs =
                configuration.get(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        this.allocateStrategy =
                AllocateStrategyFactory.getStrategy(
                        configuration, context, rocketMQSourceEnumState);
        this.initialDiscoveryFinished = rocketMQSourceEnumState.initialDiscoveryFinished();

        // For rocketmq setting
        this.groupId = configuration.get(RocketMQConnectorOptions.GROUP);
    }

    @Override
    public void start() {
        consumer = new RocketMQConsumer(configuration);
        if (Objects.nonNull(startingOffsetsInitializer.getConsumeFromWhere())) {
            consumer.setConsumeFromFirst();
        }
        consumer.start();

        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    groupId,
                    partitionDiscoveryIntervalMs);

            context.callAsync(
                    this::getSubscribedMessageQueue,
                    this::checkPartitionChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    groupId);

            context.callAsync(this::getSubscribedMessageQueue, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the rocketmq source pushes splits eagerly, rather than act upon split requests
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<RocketMQPartitionSplit> splits, int subtaskId) {
        this.addPartitionSplitChangeToPendingAssignments(splits);
        // If the failed subtask has already restarted, we need to assign splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to RocketMQSourceEnumerator for consumer group {}.",
                subtaskId,
                groupId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public RocketMQSourceEnumState snapshotState(long checkpointId) {
        return new RocketMQSourceEnumState(
                assignedPartitions, unassignedInitialPartitions, initialDiscoveryFinished);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
            } catch (Exception e) {
                LOG.error("Shutdown rocketmq internal consumer error", e);
            }
        }
    }

    // ----------------- private methods -------------------

    private Set<MessageQueue> getSubscribedMessageQueue() {
        Set<String> topicSet =
                Sets.newHashSet(
                        configuration
                                .getOptional(RocketMQOptions.TOPIC)
                                .orElseGet(Collections::emptyList));

        return topicSet.stream()
                .flatMap(topic -> consumer.partitionsFor(topic).stream())
                .collect(Collectors.toSet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void checkPartitionChanges(Set<MessageQueue> fetchedPartitions, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list subscribed MessageQueue due to ", t);
        }

        if (!initialDiscoveryFinished) {
            unassignedInitialPartitions.addAll(fetchedPartitions);
            initialDiscoveryFinished = true;
        }

        final PartitionChange partitionChange = getPartitionChange(fetchedPartitions);
        if (partitionChange.isEmpty()) {
            LOG.debug("Skip handle source allocated due to not queue change");
            return;
        }

        context.callAsync(
                () -> initializePartitionSplits(partitionChange),
                this::handlePartitionSplitChanges);
    }

    // This method should only be invoked in the coordinator executor thread.
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        Set<MessageQueue> newPartitions = partitionChange.getNewPartitions();

        OffsetsInitializer.MessageQueueOffsetsRetriever offsetsRetriever =
                new RemotingOffsetsRetriever(consumer);

        Map<MessageQueue, Long> startingOffsets = new HashMap<>();
        startingOffsets.putAll(
                newDiscoveryOffsetsInitializer.getMessageQueueOffsets(
                        newPartitions, offsetsRetriever));
        startingOffsets.putAll(
                startingOffsetsInitializer.getMessageQueueOffsets(
                        unassignedInitialPartitions, offsetsRetriever));

        Map<MessageQueue, Long> stoppingOffsets =
                stoppingOffsetsInitializer.getMessageQueueOffsets(newPartitions, offsetsRetriever);

        Set<RocketMQPartitionSplit> partitionSplits =
                newPartitions.stream()
                        .map(
                                mq -> {
                                    long startingOffset = startingOffsets.get(mq);
                                    long stoppingOffset =
                                            stoppingOffsets.getOrDefault(
                                                    mq, RocketMQPartitionSplit.NO_STOPPING_OFFSET);
                                    return new RocketMQPartitionSplit(
                                            mq, startingOffset, stoppingOffset);
                                })
                        .collect(Collectors.toSet());

        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    /**
     * Mark partition splits initialized by {@link
     * RocketMQSourceEnumerator#initializePartitionSplits(PartitionChange)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param partitionSplitChange Partition split changes
     * @param t Exception in worker thread
     */
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.info("Split changes, but dynamic partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.getNewPartitionSplits());
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    /** Calculate new split assignment according allocate strategy */
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<RocketMQPartitionSplit> newPartitionSplits) {
        Map<Integer, Set<RocketMQPartitionSplit>> newSourceSplitAllocateMap =
                this.allocateStrategy.allocate(newPartitionSplits, context.currentParallelism());

        newSourceSplitAllocateMap.forEach(
                (key, value) ->
                        pendingPartitionSplitAssignment
                                .computeIfAbsent(key, r -> new HashSet<>())
                                .addAll(value));
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<RocketMQPartitionSplit>> incrementalAssignment = new HashMap<>();

        for (Integer pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            final Set<RocketMQPartitionSplit> pendingAssignmentForReader =
                    this.pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, k -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);
                // Mark pending partitions as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            assignedPartitions.add(split.getMessageQueue());
                            unassignedInitialPartitions.remove(split.getMessageQueue());
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Enumerator assigning split(s) to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (noMoreNewPartitionSplits && this.boundedness == Boundedness.BOUNDED) {
            LOG.info(
                    "No more rocketmq partition to assign. "
                            + "Sending NoMoreSplitsEvent to the reader {} in consumer group {}.",
                    pendingReaders,
                    groupId);
            pendingReaders.forEach(this.context::signalNoMoreSplits);
        }
    }

    private void checkReaderRegistered(Integer pendingReader) {
        if (!context.registeredReaders().containsKey(pendingReader)) {
            throw new IllegalStateException(
                    String.format(
                            "Reader %d is not registered to source coordinator", pendingReader));
        }
    }

    @VisibleForTesting
    private PartitionChange getPartitionChange(Set<MessageQueue> fetchedPartitions) {

        Set<MessageQueue> removedPartitions = new HashSet<>();

        Consumer<MessageQueue> dedupOrMarkAsRemoved =
                partition -> {
                    if (!fetchedPartitions.remove(partition)) {
                        removedPartitions.add(partition);
                    }
                };

        assignedPartitions.forEach(dedupOrMarkAsRemoved);

        pendingPartitionSplitAssignment.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> dedupOrMarkAsRemoved.accept(split.getMessageQueue())));

        if (!fetchedPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitions);
        }
        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }
        return new PartitionChange(fetchedPartitions, removedPartitions);
    }

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    private static class PartitionChange {
        private final Set<MessageQueue> newPartitions;
        private final Set<MessageQueue> removedPartitions;

        public PartitionChange(
                Set<MessageQueue> newPartitions, Set<MessageQueue> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<MessageQueue> getNewPartitions() {
            return newPartitions;
        }

        public Set<MessageQueue> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    @VisibleForTesting
    public static class PartitionSplitChange {

        private final Set<RocketMQPartitionSplit> newPartitionSplits;
        private final Set<MessageQueue> removedPartitions;

        private PartitionSplitChange(Set<RocketMQPartitionSplit> newPartitionSplits) {
            this(newPartitionSplits, Collections.emptySet());
        }

        private PartitionSplitChange(
                Set<RocketMQPartitionSplit> newPartitionSplits,
                Set<MessageQueue> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }

        public Set<RocketMQPartitionSplit> getNewPartitionSplits() {
            return newPartitionSplits;
        }

        public Set<MessageQueue> getRemovedPartitions() {
            return removedPartitions;
        }
    }
}
