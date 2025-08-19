package com.tjmg.kafka.connect.lpadmerge;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LpadMergeTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(LpadMergeTransformer.class);

    private static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DOC = "Comma-separated list of field_name:length pairs to concatenate (e.g., 'field1:10,key.field2:5'). If length is omitted, no LPAD is applied.";
    private static final String MERGED_FIELD_NAME_CONFIG = "merged.name";
    private static final String MERGED_FIELD_NAME_DOC = "Name of the new merged field.";
    private static final String MERGED_FIELD_SEPARATOR_CONFIG = "merged.separator";
    private static final String MERGED_FIELD_SEPARATOR_DOC = "Separator for concatenated values.";
    private static final String TARGET_TYPE_CONFIG = "target.type";
    private static final String TARGET_TYPE_DOC = "Target Kafka Connect Schema.Type for the merged field (e.g., STRING, INT64, FLOAT64, DECIMAL).";
    private static final String DECIMAL_SCALE_CONFIG = "decimal.scale";
    private static final String DECIMAL_SCALE_DOC = "Scale for DECIMAL type, if target.type is DECIMAL.";
    private static final String MERGED_TO_KEY_CONFIG = "merged.to.key";
    private static final String MERGED_TO_KEY_DOC = "If true, sets the merged value as the new record key. This also makes the transform handle tombstone events correctly.";

    private List<String> fieldsToMerge;
    private Map<String, Integer> lpadLengths;
    private String mergedField;
    private String separator;
    private Schema.Type targetType;
    private Integer decimalScale;
    private boolean isDecimalType;
    private boolean mergedToKey;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, FIELDS_DOC)
            .define(MERGED_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, MERGED_FIELD_NAME_DOC)
            .define(MERGED_FIELD_SEPARATOR_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, MERGED_FIELD_SEPARATOR_DOC)
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, Schema.Type.STRING.name(), ConfigDef.Importance.MEDIUM, TARGET_TYPE_DOC)
            .define(DECIMAL_SCALE_CONFIG, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, DECIMAL_SCALE_DOC)
            .define(MERGED_TO_KEY_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, MERGED_TO_KEY_DOC);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        this.mergedField = config.getString(MERGED_FIELD_NAME_CONFIG);
        this.separator = config.getString(MERGED_FIELD_SEPARATOR_CONFIG);
        this.mergedToKey = config.getBoolean(MERGED_TO_KEY_CONFIG);

        String targetTypeStr = config.getString(TARGET_TYPE_CONFIG).toUpperCase();
        if ("DECIMAL".equals(targetTypeStr)) {
            this.targetType = Schema.Type.BYTES;
            this.isDecimalType = true;
        } else {
            this.targetType = Schema.Type.valueOf(targetTypeStr);
            this.isDecimalType = false;
        }
        this.decimalScale = config.getInt(DECIMAL_SCALE_CONFIG);

        this.fieldsToMerge = new ArrayList<>();
        this.lpadLengths = new HashMap<>();

        String fieldsConfigStr = config.getString(FIELDS_CONFIG);
        if (!fieldsConfigStr.isEmpty()) {
            for (String pair : fieldsConfigStr.split(",")) {
                String[] parts = pair.split(":");
                String fieldName = parts[0].trim();
                this.fieldsToMerge.add(fieldName);
                if (parts.length == 2) {
                    try {
                        this.lpadLengths.put(fieldName, Integer.parseInt(parts[1].trim()));
                    } catch (NumberFormatException e) {
                        throw new ConfigException(FIELDS_CONFIG, fieldsConfigStr, "Invalid LPAD length for field '" + fieldName + "'. Must be an integer.");
                    }
                }
            }
        }

        this.schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        // If not moving to key, and it's a tombstone, pass it through.
        if (!mergedToKey && record.value() == null) {
            return record;
        }

        String mergedString = getMergedString(record);
        Object finalValue = convertToTargetType(mergedString);

        // If we need to update the key, we do it for all records (including tombstones)
        if (mergedToKey) {
            Schema newKeySchema = Schema.STRING_SCHEMA; // The new key is always a string after merge.
            Object newKeyValue = mergedString;

            // For tombstones, the value and schema are null
            if (record.value() == null) {
                return record.newRecord(record.topic(), record.kafkaPartition(), newKeySchema, newKeyValue, null, null, record.timestamp());
            }

            // For regular records, update the value and the key
            if (record.valueSchema() == null) {
                Map<String, Object> value = Requirements.requireMap(record.value(), "lpad merge");
                Map<String, Object> updatedValue = new HashMap<>(value);
                updatedValue.put(mergedField, finalValue);
                return record.newRecord(record.topic(), record.kafkaPartition(), newKeySchema, newKeyValue, null, updatedValue, record.timestamp());
            } else {
                Struct value = Requirements.requireStruct(record.value(), "lpad merge");
                Schema updatedSchema = getUpdatedSchema(value.schema());
                Struct updatedValue = new Struct(updatedSchema);
                for (Field field : record.valueSchema().fields()) {
                    updatedValue.put(field.name(), value.get(field.name()));
                }
                updatedValue.put(mergedField, finalValue);
                return record.newRecord(record.topic(), record.kafkaPartition(), newKeySchema, newKeyValue, updatedSchema, updatedValue, record.timestamp());
            }
        }

        // Original logic: only update the value, do not change the key.
        if (record.valueSchema() == null) {
            return applySchemaless(record, finalValue);
        } else {
            return applyWithSchema(record, finalValue);
        }
    }

    private R applySchemaless(R record, Object finalValue) {
        Map<String, Object> value = Requirements.requireMap(record.value(), "lpad merge");
        Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(mergedField, finalValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, updatedValue, record.timestamp());
    }

    private R applyWithSchema(R record, Object finalValue) {
        Struct value = Requirements.requireStruct(record.value(), "lpad merge");
        Schema updatedSchema = getUpdatedSchema(value.schema());
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : record.valueSchema().fields()) {
            updatedValue.put(field.name(), value.get(field.name()));
        }
        updatedValue.put(mergedField, finalValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

    private Schema getUpdatedSchema(Schema schema) {
        Schema updatedSchema = this.schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(schema);
            this.schemaUpdateCache.put(schema, updatedSchema);
        }
        return updatedSchema;
    }

    private String getMergedString(R record) {
        List<String> lpadValues = new ArrayList<>();
        for (String fieldName : fieldsToMerge) {
            Object fieldValue = getFieldValue(record, fieldName);

            String stringValue = (fieldValue != null) ? String.valueOf(fieldValue) : "";
            int lpadLength = lpadLengths.getOrDefault(fieldName, 0);
            if (lpadLength > 0) {
                stringValue = String.format("%" + lpadLength + "s", stringValue).replace(' ', '0');
            }
            lpadValues.add(stringValue);
        }
        return String.join(separator, lpadValues);
    }

    private Object getFieldValue(R record, String fieldName) {
        // For tombstones, the value is null, so we can only read from the key.
        if (record.value() == null) {
            if (fieldName.startsWith("key.")) {
                return getNestedFieldValue(record.key(), fieldName.substring(4));
            } else {
                throw new ConfigException("Cannot read value field '" + fieldName + "' from a tombstone record.");
            }
        }

        if (fieldName.startsWith("key.")) {
            return getNestedFieldValue(record.key(), fieldName.substring(4));
        }
        return getNestedFieldValue(record.value(), fieldName);
    }

    private Object getNestedFieldValue(Object source, String fieldName) {
        String[] path = fieldName.split("\\.");
        Object current = source;
        for (String part : path) {
            if (current == null) {
                return null;
            }
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
            } else if (current instanceof Struct) {
                Struct struct = (Struct) current;
                if (struct.schema().field(part) == null) {
                    return null;
                }
                current = struct.get(part);
            } else {
                return null;
            }
        }
        return current;
    }

    private Object convertToTargetType(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            if (isDecimalType) {
                BigDecimal decimalValue = new BigDecimal(value);
                if (decimalScale != null) {
                    decimalValue = decimalValue.setScale(decimalScale, BigDecimal.ROUND_HALF_UP);
                }
                return decimalValue;
            }

            switch (targetType) {
                case STRING: return value;
                case INT8: return Byte.parseByte(value);
                case INT16: return Short.parseShort(value);
                case INT32: return Integer.parseInt(value);
                case INT64: return Long.parseLong(value);
                case FLOAT32: return Float.parseFloat(value);
                case FLOAT64: return Double.parseDouble(value);
                case BYTES:
                    throw new ConfigException(TARGET_TYPE_CONFIG, targetType.name(), "Unsupported BYTES type for conversion (only DECIMAL logical type is supported).");
                default:
                    throw new ConfigException(TARGET_TYPE_CONFIG, targetType.name(), "Unsupported target type for conversion.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(MERGED_FIELD_NAME_CONFIG, value, "Failed to convert merged value to " + (isDecimalType ? "DECIMAL" : targetType.name()) + ": " + e.getMessage());
        }
    }

    private Schema makeUpdatedSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        Schema newSchema;
        if (isDecimalType) {
            newSchema = org.apache.kafka.connect.data.Decimal.builder(decimalScale).build();
        } else {
            newSchema = SchemaBuilder.type(targetType).build();
        }
        builder.field(mergedField, newSchema);

        return builder.build();
    }

    @Override
    public void close() {
        this.schemaUpdateCache = null;
    }
}
