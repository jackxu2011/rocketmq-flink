package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** A specific {@link RocketMQDeserializationSchema} for {@link RocketMQScanTableSource}. */
public class RocketMQDynamicDeserializationSchema
        implements RocketMQDeserializationSchema<RowData> {

    private final DeserializationSchema<RowData> valueDeserialization;

    private final boolean hasMetadata;

    private final boolean hasKey;

    private final OutputProjectionCollector outputCollector;

    private final TypeInformation<RowData> producedTypeInfo;

    public RocketMQDynamicDeserializationSchema(
            int physicalArity,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo) {
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.hasKey = keyProjection.length > 0;
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity, keyProjection, valueProjection, metadataConverters);
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    @Override
    public void deserialize(MessageView record, Collector<RowData> collector) throws IOException {
        // shortcut in case no output projection is required,
        // also not for a cartesian product with the keys
        if (!hasKey && !hasMetadata) {
            valueDeserialization.deserialize(record.getBody(), collector);
            return;
        }

        // project output while emitting values
        outputCollector.inputRecord = record;
        outputCollector.outputCollector = collector;

        valueDeserialization.deserialize(record.getBody(), outputCollector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    interface MetadataConverter extends Serializable {
        Object read(MessageView record);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] keyProjection;

        private final int[] valueProjection;

        private final MetadataConverter[] metadataConverters;

        private transient MessageView inputRecord;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] keyProjection,
                int[] valueProjection,
                MetadataConverter[] metadataConverters) {
            this.physicalArity = physicalArity;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            // no key defined
            if (keyProjection.length == 0) {
                emitRow(null, (GenericRowData) physicalValueRow);
                return;
            }

            // otherwise emit a value for each key
            for (String key : inputRecord.getKeys()) {
                GenericRowData physicalKeyRow = new GenericRowData(1);
                physicalKeyRow.setField(0, key);
                emitRow(physicalKeyRow, (GenericRowData) physicalValueRow);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                throw new DeserializationException(
                        "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final int metadataArity = metadataConverters.length;
            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                assert physicalKeyRow != null;
                producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
            }

            for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                producedRow.setField(
                        valueProjection[valuePos], physicalValueRow.getField(valuePos));
            }

            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputRecord));
            }

            outputCollector.collect(producedRow);
        }
    }
}
