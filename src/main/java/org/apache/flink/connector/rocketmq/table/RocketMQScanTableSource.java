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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceBuilder;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelectorNoStopping;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.table.config.BoundedOptions;
import org.apache.flink.connector.rocketmq.table.config.StartupOptions;
import org.apache.flink.connector.rocketmq.table.serialization.RocketMQDynamicDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Defines the scan table source of RocketMQ. */
public class RocketMQScanTableSource implements ScanTableSource, SupportsReadingMetadata {

    private static final String ROCKETMQ_TRANSFORMATION = "rocketMQ";

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Format for decoding values from Kafka. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the key fields and the target position in the produced row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // RocketMQ-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The RocketMQ topics to consume. */
    protected final List<String> topics;

    private final String consumerGroup;
    private final String tag;

    private final String endpoints;

    /** Properties for the RocketMQ consumer. */
    protected final Properties properties;

    /** The startup options for the contained consumer. */
    protected final StartupOptions startupOptions;

    /** The bounded options for the contained consumer. */
    protected final BoundedOptions boundedOptions;

    protected final String tableIdentifier;

    public RocketMQScanTableSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable String group,
            @Nullable String tag,
            String endpoints,
            Properties properties,
            StartupOptions startupOptions,
            BoundedOptions boundedOptions,
            String tableIdentifier) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;

        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();

        // RocketMQ-specific attributes
        this.topics = Preconditions.checkNotNull(topics, "Topic must not be null.");
        this.consumerGroup = group;
        this.tag = tag;
        this.endpoints = endpoints;
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.startupOptions =
                Preconditions.checkNotNull(startupOptions, "Startup options must not be null.");
        this.boundedOptions =
                Preconditions.checkNotNull(boundedOptions, "Bounded options must not be null.");
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final RocketMQSource<RowData> rocketMQSource =
                createRocketMQSource(valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {

                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                rocketMQSource,
                                WatermarkStrategy.noWatermarks(),
                                "RocketMQSource-" + tableIdentifier);
                providerContext.generateUid(ROCKETMQ_TRANSFORMATION).ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return rocketMQSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    protected RocketMQSource<RowData> createRocketMQSource(
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final RocketMQDeserializationSchema<RowData> deserializationSchema =
                createRocketMQDeserializationSchema(valueDeserialization, producedTypeInfo);

        final RocketMQSourceBuilder<RowData> rocketMQSourceBuilder = RocketMQSource.builder();

        rocketMQSourceBuilder.setTopics(topics);
        rocketMQSourceBuilder.setGroupId(consumerGroup);
        rocketMQSourceBuilder.setEndpoints(endpoints);
        switch (startupOptions.startupMode) {
            case EARLIEST:
                rocketMQSourceBuilder.setStartingOffsets(OffsetsSelector.earliest());
                break;
            case LATEST:
                rocketMQSourceBuilder.setStartingOffsets(OffsetsSelector.latest());
                break;
            case GROUP_OFFSETS:
                rocketMQSourceBuilder.setStartingOffsets(OffsetsSelector.committed());
                break;
            case TIMESTAMP:
                rocketMQSourceBuilder.setStartingOffsets(
                        OffsetsSelector.timestamp(startupOptions.startupTimestampMillis));
                break;
        }

        switch (boundedOptions.boundedMode) {
            case UNBOUNDED:
                rocketMQSourceBuilder.setUnbounded(new OffsetsSelectorNoStopping());
                break;
            case LATEST:
                rocketMQSourceBuilder.setBounded(OffsetsSelector.latest());
                break;
            case GROUP_OFFSETS:
                rocketMQSourceBuilder.setBounded(OffsetsSelector.committed());
                break;
            case TIMESTAMP:
                rocketMQSourceBuilder.setBounded(
                        OffsetsSelector.timestamp(boundedOptions.boundedTimestampMillis));
                break;
        }

        rocketMQSourceBuilder.setProperties(properties).setDeserializer(deserializationSchema);

        return rocketMQSourceBuilder.build();
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public DynamicTableSource copy() {
        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(
                        physicalDataType,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topics,
                        consumerGroup,
                        tag,
                        endpoints,
                        properties,
                        startupOptions,
                        boundedOptions,
                        tableIdentifier);
        tableSource.metadataKeys = metadataKeys;
        return tableSource;
    }

    @Override
    public String asSummaryString() {
        return RocketMQScanTableSource.class.getName();
    }

    private RocketMQDeserializationSchema<RowData> createRocketMQDeserializationSchema(
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final RocketMQDynamicDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(RocketMQDynamicDeserializationSchema.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = !metadataKeys.isEmpty();

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        return new RocketMQDynamicDeserializationSchema(
                adjustedPhysicalArity,
                keyProjection,
                valueDeserialization,
                adjustedValueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC("topic", DataTypes.STRING().notNull(), MessageView::getTopic),
        QUEUE("queue", DataTypes.INT().notNull(), MessageView::getQueueId),
        OFFSET("offset", DataTypes.BIGINT().notNull(), MessageView::getQueueOffset),
        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                (msg) -> TimestampData.fromEpochMillis(msg.getIngestionTime()));

        final String key;

        final DataType dataType;

        final RocketMQDynamicDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                RocketMQDynamicDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
