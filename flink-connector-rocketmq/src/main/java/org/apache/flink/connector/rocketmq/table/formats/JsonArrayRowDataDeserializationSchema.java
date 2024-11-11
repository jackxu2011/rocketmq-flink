package org.apache.flink.connector.rocketmq.table.formats;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.AbstractJsonDeserializationSchema;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;

public class JsonArrayRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {

    private static final long serialVersionUID = 1L;
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    public JsonArrayRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
        this.runtimeConverter =
                (new JsonToRowDataConverters(
                                failOnMissingField, ignoreParseErrors, timestampFormat))
                        .createConverter((LogicalType) Preconditions.checkNotNull(rowType));
    }

    public RowData deserialize(@Nullable byte[] message) throws IOException {
        throw new UnsupportedOperationException("This method should not be called.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null) {
            out.collect(null);
        } else {
            try {
                JsonNode jsonNode = this.deserializeToJsonNode(message);
                if (jsonNode.isArray()) {
                    for (JsonNode node : jsonNode) {
                        out.collect(this.convertToRowData(node));
                    }
                } else {
                    out.collect(this.convertToRowData(jsonNode));
                }
            } catch (Throwable t) {
                if (!this.ignoreParseErrors) {
                    throw new IOException(
                            String.format("Failed to deserialize JSON '%s'.", new String(message)),
                            t);
                }
            }
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return this.objectMapper.readTree(message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) this.runtimeConverter.convert(message);
    }
}
