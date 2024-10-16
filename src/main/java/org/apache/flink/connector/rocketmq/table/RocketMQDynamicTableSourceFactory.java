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

package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceConnectorOptions;
import org.apache.flink.connector.rocketmq.table.config.BoundedOptions;
import org.apache.flink.connector.rocketmq.table.config.StartupOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getBoundedOptions;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getCredentials;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getFilterTag;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getGroup;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getRocketMQProperties;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getStartupOptions;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getTopics;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSourceFactory} implementation to create {@link
 * RocketMQScanTableSource}.
 */
public class RocketMQDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public String factoryIdentifier() {
        return RocketMQConnectorOptionsUtil.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(RocketMQSourceConnectorOptions.TOPIC);
        requiredOptions.add(RocketMQSourceConnectorOptions.GROUP);
        requiredOptions.add(RocketMQSourceConnectorOptions.ENDPOINTS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(RocketMQSourceConnectorOptions.SCAN_FILTER_TAG);
        optionalOptions.add(RocketMQSourceConnectorOptions.SCAN_STARTUP_MODE);
        optionalOptions.add(RocketMQSourceConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        optionalOptions.add(RocketMQSourceConnectorOptions.SCAN_BOUNDED_MODE);
        optionalOptions.add(RocketMQSourceConnectorOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_TIME_ZONE);
        optionalOptions.add(RocketMQSourceConnectorOptions.PULL_TIMEOUT_MILLIS);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_ENCODING);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_FIELD_DELIMITER);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_LINE_DELIMITER);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_COLUMN_ERROR_DEBUG);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_LENGTH_CHECK);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_ACCESS_KEY);
        optionalOptions.add(RocketMQSourceConnectorOptions.OPTIONAL_SECRET_KEY);
        return optionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        RocketMQSourceConnectorOptions.ENDPOINTS,
                        RocketMQSourceConnectorOptions.GROUP,
                        RocketMQSourceConnectorOptions.TOPIC,
                        RocketMQSourceConnectorOptions.SCAN_STARTUP_MODE,
                        RocketMQSourceConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        RocketMQSourceConnectorOptions.SINK_PARALLELISM,
                        RocketMQSourceConnectorOptions.TRANSACTIONAL_ID_PREFIX)
                .collect(Collectors.toSet());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validate();

        final ReadableConfig tableOptions = helper.getOptions();

        validateTableSourceOptions(tableOptions);

        final StartupOptions startupOptions = getStartupOptions(tableOptions);

        final BoundedOptions boundedOptions = getBoundedOptions(tableOptions);

        final Properties properties = getRocketMQProperties(context.getCatalogTable().getOptions());

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return new RocketMQScanTableSource(
                physicalDataType,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                getTopics(tableOptions),
                getGroup(tableOptions),
                getFilterTag(tableOptions),
                tableOptions.get(ENDPOINTS),
                getCredentials(tableOptions),
                properties,
                startupOptions,
                boundedOptions,
                context.getObjectIdentifier().asSummaryString());
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }
}
