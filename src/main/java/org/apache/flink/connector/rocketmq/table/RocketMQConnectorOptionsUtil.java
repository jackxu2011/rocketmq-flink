package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.table.config.BoundedMode;
import org.apache.flink.connector.rocketmq.table.config.BoundedOptions;
import org.apache.flink.connector.rocketmq.table.config.StartupMode;
import org.apache.flink.connector.rocketmq.table.config.StartupOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.FILTER_SQL;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.FILTER_TAG;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.GROUP;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.KEY_FIELDS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_BOUNDED_MODE;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.TOPIC;
import static org.apache.flink.connector.rocketmq.table.RocketMQConnectorOptions.VALUE_FIELDS_INCLUDE;

public class RocketMQConnectorOptionsUtil {

    public static final String IDENTIFIER = "rocketmq";
    public static final String CLIENT_CONFIG_PREFIX = "rocketmq.";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------
    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateFilterOptions(tableOptions);
        validateConsumerGroup(tableOptions);
        validateScanStartupMode(tableOptions);
        validateScanBoundedMode(tableOptions);
    }

    private static void validateConsumerGroup(ReadableConfig tableOptions) {
        if (!tableOptions.getOptional(GROUP).isPresent()) {
            throw new ValidationException(
                    String.format("Option '%s' is required in consumer group mode.", GROUP.key()));
        }
    }

    private static void validateFilterOptions(ReadableConfig tableOptions) {
        Optional<String> filterTag = tableOptions.getOptional(FILTER_TAG);
        Optional<String> filterSql = tableOptions.getOptional(FILTER_SQL);
        if (filterTag.isPresent() && filterSql.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Only one of '%s' and '%s' can be provided.",
                            FILTER_TAG.key(), FILTER_SQL.key()));
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            if (mode == RocketMQConnectorOptions.ScanStartupMode.TIMESTAMP) {
                                if (!tableOptions
                                        .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                        .isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' is required in '%s' startup mode"
                                                            + " but missing.",
                                                    SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                    RocketMQConnectorOptions.ScanStartupMode
                                                            .TIMESTAMP));
                                }
                            }
                        });
    }

    private static void validateScanBoundedMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_BOUNDED_MODE)
                .ifPresent(
                        mode -> {
                            if (mode == RocketMQConnectorOptions.ScanBoundedMode.TIMESTAMP) {
                                if (!tableOptions
                                        .getOptional(SCAN_BOUNDED_TIMESTAMP_MILLIS)
                                        .isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' is required in '%s' bounded mode"
                                                            + " but missing.",
                                                    SCAN_BOUNDED_TIMESTAMP_MILLIS.key(),
                                                    RocketMQConnectorOptions.ScanBoundedMode
                                                            .TIMESTAMP));
                                }
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static List<String> getTopics(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).orElse(null);
    }

    public static String getGroup(ReadableConfig tableOptions) {
        return tableOptions.getOptional(GROUP).orElse(null);
    }

    private static boolean isSingleTopic(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).map(t -> t.size() == 1).orElse(false);
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {

        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(RocketMQConnectorOptionsUtil::fromOption)
                        .orElse(StartupMode.GROUP_OFFSETS);
        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    public static BoundedOptions getBoundedOptions(ReadableConfig tableOptions) {
        final BoundedMode boundedMode =
                RocketMQConnectorOptionsUtil.fromOption(tableOptions.get(SCAN_BOUNDED_MODE));

        final BoundedOptions options = new BoundedOptions();
        options.boundedMode = boundedMode;
        if (boundedMode == BoundedMode.TIMESTAMP) {
            options.boundedTimestampMillis = tableOptions.get(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        }
        return options;
    }

    /**
     * Returns the {@link StartupMode} of Kafka Consumer by passed-in table-specific {@link
     * RocketMQConnectorOptions.ScanStartupMode}.
     */
    private static StartupMode fromOption(
            RocketMQConnectorOptions.ScanStartupMode scanStartupMode) {
        switch (scanStartupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;

            default:
                throw new TableException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
    }

    /**
     * Returns the {@link BoundedMode} of Kafka Consumer by passed-in table-specific {@link
     * RocketMQConnectorOptions.ScanBoundedMode}.
     */
    private static BoundedMode fromOption(
            RocketMQConnectorOptions.ScanBoundedMode scanBoundedMode) {
        switch (scanBoundedMode) {
            case UNBOUNDED:
                return BoundedMode.UNBOUNDED;
            case LATEST_OFFSET:
                return BoundedMode.LATEST;
            case GROUP_OFFSETS:
                return BoundedMode.GROUP_OFFSETS;
            case TIMESTAMP:
                return BoundedMode.TIMESTAMP;
            default:
                throw new TableException(
                        "Unsupported bounded mode. Validator should have checked that.");
        }
    }

    public static Properties getRocketMQProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();

        if (hasRocketMQClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(CLIENT_CONFIG_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey =
                                        key.substring((CLIENT_CONFIG_PREFIX).length());
                                kafkaProperties.put(subKey, value);
                            });
        }
        return kafkaProperties;
    }

    /**
     * Decides if the table options contains RocketMQ client properties that start with prefix
     * CLIENT_CONFIG_PREFIX.
     */
    private static boolean hasRocketMQClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(CLIENT_CONFIG_PREFIX));
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     *
     * <p>See {@link RocketMQConnectorOptions#KEY_FIELDS}, and {@link
     * RocketMQConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> keyFields = optionalKeyFields.orElseGet(Collections::emptyList);
        if (keyFields.isEmpty()) {
            return new int[0];
        }
        if (keyFields.size() > 1) {
            throw new ValidationException(
                    "RocketMQ supports only one key field. "
                            + "Please configure the key field by setting the 'key.fields' option.");
        }
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in '%s' must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                KEY_FIELDS.key(),
                                                keyPrefix,
                                                KEY_FIELDS_PREFIX.key(),
                                                keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link RocketMQConnectorOptions#VALUE_FORMAT}, {@link
     * RocketMQConnectorOptions#VALUE_FIELDS_INCLUDE}, and {@link
     * RocketMQConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final RocketMQConnectorOptions.ValueFieldsStrategy strategy =
                options.get(VALUE_FIELDS_INCLUDE);
        if (strategy == RocketMQConnectorOptions.ValueFieldsStrategy.ALL) {
            if (!keyPrefix.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "A key prefix is not allowed when option '%s' is set to '%s'. "
                                        + "Set it to '%s' instead to avoid field overlaps.",
                                VALUE_FIELDS_INCLUDE.key(),
                                RocketMQConnectorOptions.ValueFieldsStrategy.ALL,
                                RocketMQConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY));
            }
            return physicalFields.toArray();
        } else if (strategy == RocketMQConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    private RocketMQConnectorOptionsUtil() {}
}
