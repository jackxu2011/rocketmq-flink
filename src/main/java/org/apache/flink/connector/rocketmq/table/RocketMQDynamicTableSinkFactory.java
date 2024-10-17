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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkConnectorOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSinkFactory} implementation to create {@link
 * RocketMQDynamicTableSink}.
 */
public class RocketMQDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return RocketMQConnectorOptionsUtil.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(RocketMQSinkConnectorOptions.TOPIC);
        requiredOptions.add(RocketMQSinkConnectorOptions.ENDPOINTS);

        // requiredOptions.add(PERSIST_OFFSET_INTERVAL);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(RocketMQSinkConnectorOptions.TAG);
        optionalOptions.add(RocketMQSinkConnectorOptions.GROUP);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_RETRY_TIMES);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_SLEEP_TIME_MS);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_IS_DYNAMIC_TAG);
        optionalOptions.add(
                RocketMQSinkConnectorOptions.OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_KEYS_TO_BODY);
        optionalOptions.add(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_KEY_COLUMNS);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration properties = Configuration.fromMap(rawProperties);
        List<String> topicName =
                properties
                        .getOptional(RocketMQSinkConnectorOptions.TOPIC)
                        .orElseGet(Collections::emptyList);
        String producerGroup = properties.getString(RocketMQSinkConnectorOptions.PRODUCER_GROUP);
        String nameServerAddress = properties.getString(RocketMQSinkConnectorOptions.ENDPOINTS);
        String tag = properties.getString(RocketMQSinkConnectorOptions.TAG);
        String dynamicColumn =
                properties.getString(
                        RocketMQSinkConnectorOptions.OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
        int retryTimes =
                properties.getInteger(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_RETRY_TIMES);
        long sleepTimeMs =
                properties.getLong(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_SLEEP_TIME_MS);
        boolean isDynamicTag =
                properties.getBoolean(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_IS_DYNAMIC_TAG);
        boolean isDynamicTagIncluded =
                properties.getBoolean(
                        RocketMQSinkConnectorOptions
                                .OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED);
        boolean writeKeysToBody =
                properties.getBoolean(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_KEYS_TO_BODY);
        String keyColumnsConfig =
                properties.getString(RocketMQSinkConnectorOptions.OPTIONAL_WRITE_KEY_COLUMNS);
        String[] keyColumns = new String[0];
        if (keyColumnsConfig != null && keyColumnsConfig.length() > 0) {
            keyColumns = keyColumnsConfig.split(",");
        }
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new RocketMQDynamicTableSink(
                descriptorProperties,
                physicalSchema,
                topicName,
                producerGroup,
                nameServerAddress,
                tag,
                dynamicColumn,
                retryTimes,
                sleepTimeMs,
                isDynamicTag,
                isDynamicTagIncluded,
                writeKeysToBody,
                keyColumns);
    }
}
