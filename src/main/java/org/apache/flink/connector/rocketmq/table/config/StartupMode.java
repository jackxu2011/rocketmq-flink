package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/** Startup modes for the RocketMQ Consumer. */
@Internal
public enum StartupMode {
    /** Start from committed offsets brokers of a specific consumer group (default). */
    GROUP_OFFSETS(null),

    /** Start from the earliest offset possible. */
    EARLIEST(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET),

    /** Start from the latest offset. */
    LATEST(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET),

    /** Start from user-supplied timestamp for consumer group. */
    TIMESTAMP(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);

    /** The sentinel offset value corresponding to this startup mode. */
    private final ConsumeFromWhere consumeFromWhere;

    StartupMode(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }
}
