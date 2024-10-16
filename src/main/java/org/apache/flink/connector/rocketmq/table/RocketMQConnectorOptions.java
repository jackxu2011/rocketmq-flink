/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.rocketmq.client.AccessChannel;

import java.util.List;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.CLIENT_CONFIG_PREFIX;

/**
 * Configuration for RocketMQ Client, these config options would be used for both source, sink and
 * table.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "RocketMQClient", keyPrefix = CLIENT_CONFIG_PREFIX),
        })
/** <a href="https://rocketmq.apache.org/zh/docs/4.x/parameterConfiguration/01local">...</a> */
public class RocketMQConnectorOptions {

    //    private RocketMQConnectorOptions() {}

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FactoryUtil.FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(ValueFieldsStrategy.class)
                    .defaultValue(ValueFieldsStrategy.ALL)
                    .withDescription(
                            String.format(
                                    "Defines a strategy how to deal with key columns in the data type "
                                            + "of the value format. By default, '%s' physical columns of the table schema "
                                            + "will be included in the value format which means that the key columns "
                                            + "appear in the data type for both the key and value format.",
                                    ValueFieldsStrategy.ALL));

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines a custom prefix for all fields of the key format to avoid "
                                                    + "name clashes with fields of the value format. "
                                                    + "By default, the prefix is empty.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "If a custom prefix is defined, both the table schema and '%s' will work with prefixed names.",
                                                    KEY_FIELDS.key()))
                                    .linebreak()
                                    .text(
                                            "When constructing the data type of the key format, the prefix "
                                                    + "will be removed and the non-prefixed names will be used within the key format.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "Please note that this option requires that '%s' must be '%s'.",
                                                    VALUE_FIELDS_INCLUDE.key(),
                                                    ValueFieldsStrategy.EXCEPT_KEY))
                                    .build());

    // --------------------------------------------------------------------------------------------
    // RocketMQ specific options
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. "
                                    + "When the table is used as sink, the topic name is the topic to write data. It not supports topic list for sinks. ");

    public static final ConfigOption<String> GROUP =
            ConfigOptions.key("group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A consumer group is a load balancing group that contains consumers that use the same consumption behaviors."
                                    + "also support producer group which is discontinued.");

    /**
     * rocketmq v4 endpoints means nameserver address rocketmq v5 endpoints means proxy server
     * address
     */
    public static final ConfigOption<String> ENDPOINTS =
            ConfigOptions.key("endpoints")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ server address");

    // the config of session credential
    public static final ConfigOption<String> OPTIONAL_ACCESS_KEY =
            ConfigOptions.key("accessKey").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SECRET_KEY =
            ConfigOptions.key("secretKey").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> GLOBAL_DEBUG_MODE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "debug").booleanType().defaultValue(false);

    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ instance namespace");

    /** 这里不知道对轨迹功能有没有影响, 待验证 */
    public static final ConfigOption<AccessChannel> OPTIONAL_ACCESS_CHANNEL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "channel")
                    .enumType(AccessChannel.class)
                    .defaultValue(AccessChannel.CLOUD)
                    .withDescription("RocketMQ access channel");

    public static final ConfigOption<Integer> CLIENT_CALLBACK_EXECUTOR_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "callback.threads")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "The number of processor cores "
                                    + "when the client communication layer receives a network request");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "partition.discovery.interval.ms")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Time interval for polling route information from nameserver or proxy");

    public static final ConfigOption<Long> HEARTBEAT_INTERVAL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "heartbeat.interval.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Interval for regularly sending registration heartbeats to broker");

    public static final ConfigOption<Boolean> OPTIONAL_UNIT_MODE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "unitMode").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_UNIT_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "unitName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> VIP_CHANNEL_ENABLED =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "channel.vip.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable vip netty channel for sending messages");

    public static final ConfigOption<Boolean> USE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tls.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS transport.");

    public static final ConfigOption<Long> MQ_CLIENT_API_TIMEOUT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "network.timeout.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("RocketMQ client api timeout setting");

    public static final ConfigOption<String> OPTIONAL_TIME_ZONE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "timeZone").stringType().noDefaultValue();

    // for message payload
    public static final ConfigOption<String> OPTIONAL_ENCODING =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "message.encoding")
                    .stringType()
                    .defaultValue("UTF-8");

    public static final ConfigOption<String> OPTIONAL_FIELD_DELIMITER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "message.field.delimiter")
                    .stringType()
                    .defaultValue("\u0001");

    public static final ConfigOption<String> OPTIONAL_LINE_DELIMITER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "message.line.delimiter")
                    .stringType()
                    .defaultValue("\n");

    public static final ConfigOption<String> OPTIONAL_LENGTH_CHECK =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "message.length.check")
                    .stringType()
                    .defaultValue("NONE");

    public static final ConfigOption<Boolean> COMMIT_OFFSETS_ON_CHECKPOINT =
            ConfigOptions.key("commit.offsets.on.checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to commit consuming offset on checkpoint.");

    public static final ConfigOption<Long> POLL_TIMEOUT =
            ConfigOptions.key("poll.timeout")
                    .longType()
                    .defaultValue(10L)
                    .withDescription("how long to wait before giving up, the unit is milliseconds");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.GROUP_OFFSETS)
                    .withDescription("Startup mode for Kafka consumer.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<ScanBoundedMode> SCAN_BOUNDED_MODE =
            ConfigOptions.key("scan.bounded.mode")
                    .enumType(ScanBoundedMode.class)
                    .defaultValue(ScanBoundedMode.UNBOUNDED)
                    .withDescription("Bounded mode for Kafka consumer.");

    public static final ConfigOption<Long> SCAN_BOUNDED_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.bounded.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" bounded mode");

    public static final ConfigOption<String> SCAN_FILTER_TAG =
            ConfigOptions.key("scan.filter.tag")
                    .stringType()
                    .defaultValue("*")
                    .withDescription(
                            "for message filter, rocketmq only support single filter option");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<String> TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key("TRANSACTIONAL_ID_PREFIX").stringType().noDefaultValue();

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Strategies to derive the data type of a value format by considering a key format. */
    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }

    /** Startup mode for the rocketMQ consumer, see {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode implements DescribedEnum {
        EARLIEST_OFFSET("earliest-offset", text("Start from the earliest offset possible.")),
        LATEST_OFFSET("latest-offset", text("Start from the latest offset.")),
        GROUP_OFFSETS(
                "group-offsets",
                text("Start from committed offsets in brokers of a specific consumer group.")),
        TIMESTAMP("timestamp", text("Start from user-supplied timestamp for consumer group."));

        private final String value;
        private final InlineElement description;

        ScanStartupMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Bounded mode for the rocketMQ consumer, see {@link #SCAN_BOUNDED_MODE}. */
    public enum ScanBoundedMode implements DescribedEnum {
        UNBOUNDED("unbounded", text("Do not stop consuming")),
        LATEST_OFFSET(
                "latest-offset",
                text(
                        "Bounded by latest offsets. This is evaluated at the start of consumption"
                                + " from a given partition.")),
        GROUP_OFFSETS(
                "group-offsets",
                text(
                        "Bounded by committed offsets in brokers of a specific"
                                + " consumer group. This is evaluated at the start of consumption"
                                + " from a given partition.")),
        TIMESTAMP("timestamp", text("Bounded by a user-supplied timestamp."));

        private final String value;
        private final InlineElement description;

        ScanBoundedMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
