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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.table.config.BoundedOptions;
import org.apache.flink.connector.rocketmq.table.config.StartupOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.FILTER_SQL;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.FILTER_TAG;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.GROUP;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.KEY_FIELDS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_BOUNDED_MODE;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.TOPIC;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.CLIENT_CONFIG_PREFIX;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptionsUtil.getBoundedOptions;
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
        requiredOptions.add(TOPIC);
        requiredOptions.add(ENDPOINTS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(FactoryUtil.FORMAT);
        optionalOptions.add(KEY_FIELDS);
        optionalOptions.add(KEY_FIELDS_PREFIX);
        optionalOptions.add(VALUE_FORMAT);
        optionalOptions.add(VALUE_FIELDS_INCLUDE);
        optionalOptions.add(GROUP);
        optionalOptions.add(FILTER_TAG);
        optionalOptions.add(FILTER_SQL);
        optionalOptions.add(SCAN_STARTUP_MODE);
        optionalOptions.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        optionalOptions.add(SCAN_BOUNDED_MODE);
        optionalOptions.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(TRANSACTIONAL_ID_PREFIX);
        return optionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        ENDPOINTS,
                        GROUP,
                        TOPIC,
                        SCAN_STARTUP_MODE,
                        SCAN_STARTUP_TIMESTAMP_MILLIS,
                        SINK_PARALLELISM,
                        TRANSACTIONAL_ID_PREFIX)
                .collect(Collectors.toSet());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validateExcept(CLIENT_CONFIG_PREFIX);
        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueDecodingFormat);

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
                tableOptions.get(ENDPOINTS),
                properties,
                startupOptions,
                boundedOptions,
                context.getObjectIdentifier().asSummaryString());
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            Map<String, String> options,
            Format format) {
        if (primaryKeyIndexes.length > 0
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration configuration = Configuration.fromMap(options);
            String formatName =
                    configuration
                            .getOptional(FactoryUtil.FORMAT)
                            .orElse(configuration.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
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
