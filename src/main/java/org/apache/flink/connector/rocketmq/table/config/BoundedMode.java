package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;

/** End modes for the RocketMQ Consumer. */
@Internal
public enum BoundedMode {

    /** Do not end consuming. */
    UNBOUNDED,

    /**
     * End from committed offsets in brokers of a specific consumer group. This is evaluated at the
     * start of consumption from a given partition.
     */
    GROUP_OFFSETS(RocketMQPartitionSplit.COMMITTED_OFFSET),

    /**
     * End from the latest offset. This is evaluated at the start of consumption from a given
     * partition.
     */
    LATEST(RocketMQPartitionSplit.LATEST_OFFSET),

    /** End from user-supplied timestamp for each partition. */
    TIMESTAMP;

    /** The sentinel offset value corresponding to this startup mode. */
    private final long stateSentinel;

    BoundedMode() {
        this.stateSentinel = RocketMQPartitionSplit.NO_STOPPING_OFFSET;
    }

    BoundedMode(long stateSentinel) {
        this.stateSentinel = stateSentinel;
    }

    public long getStateSentinel() {
        return stateSentinel;
    }
}
