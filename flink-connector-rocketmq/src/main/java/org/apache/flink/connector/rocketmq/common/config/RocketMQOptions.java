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

package org.apache.flink.connector.rocketmq.common.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.rocketmq.client.AccessChannel;

import java.util.List;

/**
 * Configuration for RocketMQ Client, these config options would be used for both source, sink and
 * table.
 */
@PublicEvolving
/** <a href="https://rocketmq.apache.org/zh/docs/4.x/parameterConfiguration/01local">...</a> */
public class RocketMQOptions {

    private RocketMQOptions() {}

    // --------------------------------------------------------------------------------------------
    // RocketMQ specific options
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key(RocketMQConfig.TOPIC)
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. "
                                    + "When the table is used as sink, the topic name is the topic to write data. It not supports topic list for sinks. ");

    public static final ConfigOption<String> GROUP =
            ConfigOptions.key(RocketMQConfig.GROUP)
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
            ConfigOptions.key(RocketMQConfig.ENDPOINTS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ server address");

    // the config of session credential
    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key(RocketMQConfig.ACCESS_KEY).stringType().noDefaultValue();

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key(RocketMQConfig.SECRET_KEY).stringType().noDefaultValue();

    public static final ConfigOption<String> CLIENT_ID_PREFIX =
            ConfigOptions.key("client.id.prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prefix to use for the rocketmq client.");

    public static final ConfigOption<String> FILTER_TAG =
            ConfigOptions.key(RocketMQConfig.FILTER_TAG)
                    .stringType()
                    .defaultValue(RocketMQConfig.DEFAULT_FILTER_TAG)
                    .withDescription(
                            "for message filter, rocketmq assign mode only support tag filter option");

    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ instance namespace");

    /** 这里不知道对轨迹功能有没有影响, 待验证 */
    public static final ConfigOption<AccessChannel> ACCESS_CHANNEL =
            ConfigOptions.key(RocketMQConfig.ACCESS_CHANNEL)
                    .enumType(AccessChannel.class)
                    .defaultValue(RocketMQConfig.DEFAULT_ACCESS_CHANNEL)
                    .withDescription("RocketMQ access channel");

    public static final ConfigOption<Integer> CLIENT_CALLBACK_EXECUTOR_THREADS =
            ConfigOptions.key("callback.threads")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "The number of processor cores "
                                    + "when the client communication layer receives a network request");

    public static final ConfigOption<Long> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Interval for regularly sending registration heartbeats to broker");

    public static final ConfigOption<Boolean> UNIT_MODE =
            ConfigOptions.key("unitMode").booleanType().defaultValue(false);

    public static final ConfigOption<String> UNIT_NAME =
            ConfigOptions.key("unitName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> VIP_CHANNEL_ENABLED =
            ConfigOptions.key("channel.vip.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable vip netty channel for sending messages");

    public static final ConfigOption<Boolean> USE_TLS =
            ConfigOptions.key("tls.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS transport.");

    public static final ConfigOption<Long> MQ_CLIENT_API_TIMEOUT =
            ConfigOptions.key("network.timeout.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("RocketMQ client api timeout setting");

    public static final ConfigOption<String> TIME_ZONE =
            ConfigOptions.key("timeZone").stringType().noDefaultValue();
}
