/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkBuilder;
import org.apache.flink.connector.rocketmq.table.serialization.RocketMQDynamicRecordSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.rocketmq.client.producer.MessageQueueSelector;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Defines the dynamic table sink of RocketMQ. */
public class RocketMQDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------
    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type of consumed data type. */
    protected DataType consumedDataType;

    /** Format for encoding values to Kafka. */
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    /** Indices that determine the key fields and the source position in the consumed row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the source position in the consumed row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // RocketMQ-specific attributes
    // --------------------------------------------------------------------------------------------

    private final List<String> topics;
    private final String producerGroup;
    private final String endpoints;

    private final Properties properties;

    /** Partitioner to select Kafka partition for each item. */
    protected final @Nullable MessageQueueSelector partitioner;

    /** Parallelism of the physical Kafka producer. * */
    protected final @Nullable Integer parallelism;

    public RocketMQDynamicSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            List<String> topics,
            String producerGroup,
            String endpoints,
            Properties properties,
            @Nullable MessageQueueSelector partitioner,
            @Nullable Integer parallelism) {
        // Mutable attributes
        this.metadataKeys = Collections.emptyList();
        this.physicalDataType =
                checkNotNull(physicalDataType, "Consumed data type must not be null.");
        // Format attributes
        this.consumedDataType = physicalDataType;
        this.valueEncodingFormat =
                checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
        this.keyProjection = checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection = checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;

        // RocketMQ-specific attributes
        this.topics = checkNotNull(topics, "Topics must not be null.");
        this.producerGroup = producerGroup;
        this.endpoints = checkNotNull(endpoints, "Endpoints must not be null.");
        this.properties = properties;

        this.partitioner = partitioner;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(
            DynamicTableSink.Context context) {

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection);

        final RocketMQSinkBuilder<RowData> sinkBuilder = RocketMQSink.builder();

        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RocketMQSink<RowData> sink =
                sinkBuilder
                        .setEndpoints(endpoints)
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperties(properties)
                        .setGroupId(producerGroup)
                        .setPartitioner(partitioner)
                        .setSerializer(
                                new RocketMQDynamicRecordSerializationSchema(
                                        topics,
                                        valueSerialization,
                                        getFieldGetters(physicalChildren, keyProjection),
                                        getFieldGetters(physicalChildren, valueProjection),
                                        hasMetadata(),
                                        getMetadataPositions(physicalChildren)))
                        .build();

        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public DynamicTableSink copy() {
        RocketMQDynamicSink tableSink =
                new RocketMQDynamicSink(
                        physicalDataType,
                        valueEncodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topics,
                        producerGroup,
                        endpoints,
                        properties,
                        partitioner,
                        parallelism);
        tableSink.consumedDataType = consumedDataType;
        tableSink.metadataKeys = metadataKeys;
        return tableSink;
    }

    @Override
    public String asSummaryString() {
        return RocketMQDynamicSink.class.getName();
    }

    private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = metadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            }
                            return physicalChildren.size() + pos;
                        })
                .toArray();
    }

    private boolean hasMetadata() {
        return !metadataKeys.isEmpty();
    }

    private RowData.FieldGetter[] getFieldGetters(
            List<LogicalType> physicalChildren, int[] keyProjection) {
        return Arrays.stream(keyProjection)
                .mapToObj(
                        targetField ->
                                RowData.createFieldGetter(
                                        physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    public enum WritableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                }),
        KEYS(
                "keys",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                }),
        TAGS(
                "tags",
                DataTypes.STRING().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                });

        final String key;

        final DataType dataType;

        public final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    public interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
