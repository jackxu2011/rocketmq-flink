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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RocketMQDynamicTableFactory}. */
public class RocketMQDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Collections.singletonList(Column.physical("name", STRING().notNull())),
                    new ArrayList<>(),
                    null);

    private static final String TOPIC = "test_source";
    private static final String CONSUMER_GROUP = "test_consumer";
    private static final String NAME_SERVER_ADDRESS = "127.0.0.1:9876";

    private static DynamicTableSource createTableSource(
            Map<String, String> options, Configuration conf) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", RocketMQConnectorOptionsUtil.IDENTIFIER),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(SCHEMA).build(),
                                "mock source",
                                Collections.emptyList(),
                                options),
                        SCHEMA),
                conf,
                RocketMQDynamicTableFactory.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(options, new Configuration());
    }

    @Test
    public void testRocketMQDynamicTableSourceWithLegalOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", RocketMQConnectorOptionsUtil.IDENTIFIER);
        options.put(RocketMQConnectorOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQConnectorOptions.GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQConnectorOptions.ENDPOINTS.key(), NAME_SERVER_ADDRESS);
        options.put(FactoryUtil.FORMAT.key(), "csv");
        options.put(
                RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        final DynamicTableSource tableSource = createTableSource(options);
        assertTrue(tableSource instanceof RocketMQDynamicSource);
        assertEquals(RocketMQDynamicSource.class.getName(), tableSource.asSummaryString());
    }

    @Ignore
    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSourceWithoutRequiredOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", RocketMQConnectorOptionsUtil.IDENTIFIER);
        options.put(RocketMQConnectorOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQConnectorOptions.GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQConnectorOptions.FILTER_TAG.key(), "test_tag");
        createTableSource(options);
    }

    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSourceWithUnknownOption() {
        final Map<String, String> options = new HashMap<>();
        options.put(RocketMQConnectorOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQConnectorOptions.GROUP.key(), CONSUMER_GROUP);
        // options.put(RocketMQSourceOptions.PERSIST_OFFSET_INTERVAL.key(), NAME_SERVER_ADDRESS);
        options.put("unknown", "test_option");
        createTableSource(options);
    }

    private static DynamicTableSink createDynamicTableSink(Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "mq"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(SCHEMA).build(),
                                "mock sink",
                                Collections.emptyList(),
                                options),
                        SCHEMA),
                new Configuration(),
                RocketMQDynamicTableFactory.class.getClassLoader(),
                false);
    }

    @Ignore
    @Test
    public void testRocketMQDynamicTableSinkWithLegalOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", RocketMQConnectorOptionsUtil.IDENTIFIER);
        options.put(RocketMQConnectorOptions.TOPIC.key(), TOPIC);
        options.put(
                RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        final DynamicTableSink tableSink = createDynamicTableSink(options);
        assertTrue(tableSink instanceof RocketMQDynamicSink);
        assertEquals(RocketMQDynamicSink.class.getName(), tableSink.asSummaryString());
    }

    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSinkWithoutRequiredOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", RocketMQConnectorOptionsUtil.IDENTIFIER);
        options.put(RocketMQConfig.TOPIC, TOPIC);
        options.put(
                RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        options.put(RocketMQConnectorOptions.FILTER_TAG.key(), "test_tag");
        createDynamicTableSink(options);
    }

    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSinkWithUnknownOption() {
        final Map<String, String> options = new HashMap<>();
        options.put(RocketMQConnectorOptions.TOPIC.key(), TOPIC);
        options.put(
                RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        options.put("unknown", "test_option");
        createDynamicTableSink(options);
    }
}
