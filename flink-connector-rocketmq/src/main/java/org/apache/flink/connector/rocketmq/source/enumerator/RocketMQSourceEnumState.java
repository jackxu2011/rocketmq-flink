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

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** The state of RocketMQ source enumerator. */
@Internal
public class RocketMQSourceEnumState {

    /** Partitions with status: ASSIGNED or UNASSIGNED_INITIAL. */
    private final Set<MessageQueueWithAssignmentStatus> partitions;

    /**
     * this flag will be marked as true if initial partitions are discovered after enumerator
     * starts.
     */
    private final boolean initialDiscoveryFinished;

    public RocketMQSourceEnumState(
            Set<MessageQueueWithAssignmentStatus> partitions, boolean initialDiscoveryFinished) {
        this.partitions = partitions;
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public RocketMQSourceEnumState(
            Set<MessageQueue> assignPartitions,
            Set<MessageQueue> unassignedInitialPartitions,
            boolean initialDiscoveryFinished) {
        this.partitions = new HashSet<>();
        partitions.addAll(
                assignPartitions.stream()
                        .map(
                                topicPartition ->
                                        new MessageQueueWithAssignmentStatus(
                                                topicPartition, AssignmentStatus.ASSIGNED))
                        .collect(Collectors.toSet()));
        partitions.addAll(
                unassignedInitialPartitions.stream()
                        .map(
                                topicPartition ->
                                        new MessageQueueWithAssignmentStatus(
                                                topicPartition,
                                                AssignmentStatus.UNASSIGNED_INITIAL))
                        .collect(Collectors.toSet()));
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public Set<MessageQueueWithAssignmentStatus> getPartitions() {
        return partitions;
    }

    public Set<MessageQueue> assignedPartitions() {
        return filterPartitionsByAssignmentStatus(AssignmentStatus.ASSIGNED);
    }

    public Set<MessageQueue> unassignedInitialPartitions() {
        return filterPartitionsByAssignmentStatus(AssignmentStatus.UNASSIGNED_INITIAL);
    }

    public boolean initialDiscoveryFinished() {
        return initialDiscoveryFinished;
    }

    private Set<MessageQueue> filterPartitionsByAssignmentStatus(
            AssignmentStatus assignmentStatus) {
        return partitions.stream()
                .filter(
                        partitionWithStatus ->
                                partitionWithStatus.getAssignmentStatus().equals(assignmentStatus))
                .map(MessageQueueWithAssignmentStatus::getMessageQueue)
                .collect(Collectors.toSet());
    }
}
