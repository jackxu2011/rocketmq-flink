package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/** Startup modes for the RocketMQ Consumer. */
@Internal
public enum StartupMode {
    /** Start from committed offsets brokers of a specific consumer group (default). */
    GROUP_OFFSETS(null, RocketMQPartitionSplit.COMMITTED_OFFSET),

    /** Start from the earliest offset possible. */
    EARLIEST(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, RocketMQPartitionSplit.EARLIEST_OFFSET),

    /** Start from the latest offset. */
    LATEST(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, RocketMQPartitionSplit.LATEST_OFFSET),

    /** Start from user-supplied timestamp for consumer group. */
    TIMESTAMP(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);

    /** The sentinel offset value corresponding to this startup mode. */
    private final ConsumeFromWhere consumeFromWhere;

    /** The sentinel offset value corresponding to this startup mode. */
    private final long stateSentinel;

    StartupMode(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
        this.stateSentinel = Long.MIN_VALUE;
    }

    StartupMode(ConsumeFromWhere consumeFromWhere, long stateSentinel) {
        this.consumeFromWhere = consumeFromWhere;
        this.stateSentinel = stateSentinel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public long getStateSentinel() {
        return stateSentinel;
    }
}
