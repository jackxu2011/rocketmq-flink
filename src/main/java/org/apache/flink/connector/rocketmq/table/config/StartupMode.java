package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;

/** Startup modes for the RocketMQ Consumer. */
@Internal
public enum StartupMode {
    /** Start from committed offsets brokers of a specific consumer group (default). */
    GROUP_OFFSETS,

    /** Start from the earliest offset possible. */
    EARLIEST,

    /** Start from the latest offset. */
    LATEST,

    /** Start from user-supplied timestamp for consumer group. */
    TIMESTAMP
}
