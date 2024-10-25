package org.apache.flink.connector.rocketmq.table.config;

import org.apache.flink.annotation.Internal;

/** RocketMQ startup options. */
@Internal
public class StartupOptions {
    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    public StartupMode startupMode;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    public long specificOffsets;

    /**
     * The start timestamp to locate partition offsets; only relevant when startup mode is {@link
     * StartupMode#TIMESTAMP}.
     */
    public long startupTimestampMillis;
}
