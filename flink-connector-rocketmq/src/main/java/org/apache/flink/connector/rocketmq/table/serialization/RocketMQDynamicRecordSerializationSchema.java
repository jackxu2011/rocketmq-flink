package org.apache.flink.connector.rocketmq.table.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.connector.rocketmq.table.RocketMQDynamicSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RocketMQDynamicRecordSerializationSchema
        implements RocketMQSerializationSchema<RowData> {

    private final Set<String> topics;
    private final SerializationSchema<RowData> valueSerialization;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] valueFieldGetters;
    private final boolean hasMetadata;
    private final int[] metadataPositions;

    public RocketMQDynamicRecordSerializationSchema(
            List<String> topics,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions) {

        Preconditions.checkArgument(topics != null && !topics.isEmpty(), "Topic must be set.");
        this.topics = new HashSet<>(topics);
        this.valueSerialization = valueSerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
    }

    @Override
    public Message serialize(RowData consumedRow, RocketMQSinkContext context, Long timestamp) {
        final String topic = getTargetTopic(consumedRow);
        final Message msg = new Message();
        msg.setTopic(topic);
        if (keyFieldGetters.length == 0 && !hasMetadata) {
            final byte[] valueSerialized = valueSerialization.serialize(consumedRow);
            msg.setBody(valueSerialized);
        } else {
            final RowKind kind = consumedRow.getRowKind();

            final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
            final byte[] valueSerialized = valueSerialization.serialize(valueRow);
            msg.setBody(valueSerialized);
        }

        if (keyFieldGetters.length > 0) {
            msg.setKeys(getKeys(consumedRow, keyFieldGetters));
        }
        if (hasMetadata) {
            msg.setTags(readMetadata(consumedRow, RocketMQDynamicSink.WritableMetadata.TAGS));
            msg.setKeys(
                    (String) readMetadata(consumedRow, RocketMQDynamicSink.WritableMetadata.KEYS));
        }
        msg.setWaitStoreMsgOK(true);
        return msg;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, RocketMQSinkContext sinkContext)
            throws Exception {
        valueSerialization.open(context);
    }

    private RowData createProjectedRow(
            RowData consumedRow, RowKind kind, FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    private List<String> getKeys(RowData consumedRow, FieldGetter[] keyFieldGetters) {
        List<String> keys = new ArrayList<>();
        for (FieldGetter fieldGetter : keyFieldGetters) {
            keys.add(Objects.requireNonNull(fieldGetter.getFieldOrNull(consumedRow)).toString());
        }
        return keys;
    }

    private String getTargetTopic(RowData element) {
        if (topics != null && topics.size() == 1) {
            // If topics is a singleton list, we only return the provided topic.
            return topics.stream().findFirst().get();
        }
        final String targetTopic =
                readMetadata(element, RocketMQDynamicSink.WritableMetadata.TOPIC);
        if (targetTopic == null) {
            throw new IllegalArgumentException(
                    "The topic of the sink record is not valid. Expected a single topic but no topic is set.");
        } else if (topics != null && !topics.contains(targetTopic)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The topic of the sink record is not valid. Expected topic to be in: %s but was: %s",
                            topics, targetTopic));
        }
        return targetTopic;
    }

    private <T> T readMetadata(RowData consumedRow, RocketMQDynamicSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }
}
