package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;

/** RocketMQ bounded options. * */
@Internal
public class BoundedOptions {
    /** The bounded mode for the contained consumer (default is an unbounded data stream). */
    public BoundedMode boundedMode;

    /**
     * The bounded timestamp to locate partition offsets; only relevant when bounded mode is {@link
     * BoundedMode#TIMESTAMP}.
     */
    public long boundedTimestampMillis;
}
