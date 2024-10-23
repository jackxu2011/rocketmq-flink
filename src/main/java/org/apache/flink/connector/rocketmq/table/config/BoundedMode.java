package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;

/** End modes for the RocketMQ Consumer. */
@Internal
public enum BoundedMode {

    /** Do not end consuming. */
    UNBOUNDED,

    /**
     * End from committed offsets in brokers of a specific consumer group. This is evaluated at the
     * start of consumption from a given partition.
     */
    GROUP_OFFSETS,

    /**
     * End from the latest offset. This is evaluated at the start of consumption from a given
     * partition.
     */
    LATEST,

    /** End from user-supplied timestamp for each partition. */
    TIMESTAMP
}
