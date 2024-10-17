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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfigValidator;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategyFactory;

/** Includes config options of RocketMQ connector type. */
public class RocketMQSourceOptions {

    public static final RocketMQConfigValidator SOURCE_CONFIG_VALIDATOR =
            RocketMQConfigValidator.builder().build();

    public static final ConfigOption<Boolean> COMMIT_OFFSETS_ON_CHECKPOINT =
            ConfigOptions.key("commit.offsets.on.checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to commit consuming offset on checkpoint.");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key("partition.discovery.interval.ms")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Time interval for polling route information from nameserver or proxy");

    public static final ConfigOption<Long> POLL_TIMEOUT =
            ConfigOptions.key("poll.timeout")
                    .longType()
                    .defaultValue(10L)
                    .withDescription("how long to wait before giving up, the unit is milliseconds");

    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG =
            ConfigOptions.key("column.error.debug")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("If object deserialize failed, would print error message");

    public static final ConfigOption<String> ALLOCATE_MESSAGE_QUEUE_STRATEGY =
            ConfigOptions.key("allocate.strategy")
                    .stringType()
                    .defaultValue(AllocateStrategyFactory.STRATEGY_NAME_CONSISTENT_HASH)
                    .withDescription("The load balancing strategy algorithm");

    // pull message limit
    public static final ConfigOption<Integer> PULL_THREADS_NUM =
            ConfigOptions.key("pull.threads.num")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The number of pull threads set");

    public static final ConfigOption<Long> PULL_BATCH_SIZE =
            ConfigOptions.key("pull.batch.size")
                    .longType()
                    .defaultValue(32L)
                    .withDescription("The maximum number of messages pulled each time");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_QUEUE =
            ConfigOptions.key("pull.threshold.queue")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The queue level flow control threshold");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_ALL =
            ConfigOptions.key("pull.threshold.all")
                    .longType()
                    .defaultValue(10 * 1000L)
                    .withDescription("The threshold for flow control of consumed requests");

    public static final ConfigOption<Long> PULL_TIMEOUT_MILLIS =
            ConfigOptions.key("pull.rpc.timeout")
                    .longType()
                    .defaultValue(20 * 1000L)
                    .withDescription("The polling timeout setting");

    public static final ConfigOption<Long> PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION =
            ConfigOptions.key("pull.rpc.exception.delay")
                    .longType()
                    .defaultValue(3 * 1000L)
                    .withDescription(
                            "The maximum time that a connection will be suspended "
                                    + "for in long polling by the broker");

    public static final ConfigOption<Long> PULL_TIMEOUT_LONG_POLLING_SUSPEND =
            ConfigOptions.key("pull.suspend.timeout")
                    .longType()
                    .defaultValue(30 * 1000L)
                    .withDescription(
                            "The maximum wait time for a response from the broker "
                                    + "in long polling by the client");

    /** for auto commit offset to rocketmq server */
    public static final ConfigOption<Boolean> AUTO_COMMIT_OFFSET =
            ConfigOptions.key("offset.commit.auto")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The setting for automatic commit of offset");

    public static final ConfigOption<Long> AUTO_COMMIT_OFFSET_INTERVAL =
            ConfigOptions.key("offset.commit.interval")
                    .longType()
                    .defaultValue(5 * 1000L)
                    .withDescription(
                            "Applies to Consumer, the interval for persisting consumption progress");

    /** for message trace, suggest not enable when heavy traffic */
    public static final ConfigOption<Boolean> ENABLE_MESSAGE_TRACE =
            ConfigOptions.key("trace.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The flag for message tracing");

    public static final ConfigOption<String> CUSTOMIZED_TRACE_TOPIC =
            ConfigOptions.key("trace.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the topic for message tracing");
}
