package com.tjmg.kafka.connect.lpadmerge;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LpadMergeTransformerTest {

    @Test
    public void shouldLpadAndMergeToString() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema valSchema = SchemaBuilder.struct()
            .field("id_processo", Schema.INT64_SCHEMA)
            .field("seq_processo", Schema.INT32_SCHEMA)
            .build();

        Struct value = new Struct(valSchema);
        value.put("id_processo", 123L);
        value.put("seq_processo", 45);

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "id_processo:5,seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "STRING");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();

        assertEquals(123L, resStruct.get("id_processo"));
        assertEquals(45, resStruct.get("seq_processo"));
        assertEquals("00123045", resStruct.get("process_id_seq"));
    }

    @Test
    public void shouldLpadAndMergeToInt64() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema valSchema = SchemaBuilder.struct()
            .field("id_processo", Schema.INT64_SCHEMA)
            .field("seq_processo", Schema.INT32_SCHEMA)
            .build();

        Struct value = new Struct(valSchema);
        value.put("id_processo", 123L);
        value.put("seq_processo", 45);

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

                Map<String, String> config = new HashMap<>();
        config.put("fields", "id_processo:5,seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "INT64");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();

        assertEquals(123L, resStruct.get("id_processo"));
        assertEquals(45, resStruct.get("seq_processo"));
        assertEquals(123045L, resStruct.get("process_id_seq")); // 00123045 -> 123045
    }

            @Test
    public void shouldLpadAndMergeToDecimal() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema valSchema = SchemaBuilder.struct()
            .field("amount", Schema.FLOAT64_SCHEMA)
            .field("currency_code", Schema.STRING_SCHEMA)
            .build();

        Struct value = new Struct(valSchema);
        value.put("amount", 123.45);
        value.put("currency_code", "USD");

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "amount:11");
        config.put("merged.name", "formatted_amount");
        config.put("merged.separator", "");
        config.put("target.type", "DECIMAL");
        config.put("decimal.scale", "2");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();

        assertEquals(123.45, resStruct.get("amount"));
        assertEquals("USD", resStruct.get("currency_code"));
        assertEquals("123.45", resStruct.get("formatted_amount"));
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowErrorOnInvalidConversion() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema valSchema = SchemaBuilder.struct()
            .field("id_processo", Schema.STRING_SCHEMA)
            .build();

        Struct value = new Struct(valSchema);
        value.put("id_processo", "abc");

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "id_processo:5");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "INT64");

        transformer.configure(config);
        transformer.apply(record);
    }

    @Test
    public void shouldHandleNullValues() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema valSchema = SchemaBuilder.struct()
            .field("id_processo", Schema.OPTIONAL_INT64_SCHEMA)
            .field("seq_processo", Schema.INT32_SCHEMA)
            .build();

        Struct value = new Struct(valSchema);
        value.put("id_processo", null);
        value.put("seq_processo", 45);

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "id_processo:5,seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "STRING");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();

        assertNull(resStruct.get("id_processo"));
        assertEquals(45, resStruct.get("seq_processo"));
        assertEquals("00000045", resStruct.get("process_id_seq")); // Null becomes empty string, then lpad
    }

    @Test
    public void shouldHandleSchemalessRecords() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Map<String, Object> value = new HashMap<>();
        value.put("id_processo", 123L);
        value.put("seq_processo", 45);

        SinkRecord record = new SinkRecord("test", 1, null, "key", null, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "id_processo:5,seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "STRING");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Map<String, Object> resMap = (Map<String, Object>) result.value();

        assertEquals(123L, resMap.get("id_processo"));
        assertEquals(45, resMap.get("seq_processo"));
        assertEquals("00123045", resMap.get("process_id_seq"));
    }

    @Test
    public void shouldHandleNestedFields() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.string().build();
        Schema nestedSchema = SchemaBuilder.struct()
            .field("sub_id", Schema.INT64_SCHEMA)
            .field("sub_seq", Schema.INT32_SCHEMA)
            .build();
        Schema valSchema = SchemaBuilder.struct()
            .field("nested", nestedSchema)
            .field("other_field", Schema.STRING_SCHEMA)
            .build();

        Struct nestedValue = new Struct(nestedSchema);
        nestedValue.put("sub_id", 789L);
        nestedValue.put("sub_seq", 10);

        Struct value = new Struct(valSchema);
        value.put("nested", nestedValue);
        value.put("other_field", "test");

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "nested.sub_id:5,nested.sub_seq:3");
        config.put("merged.name", "nested_merged");
        config.put("merged.separator", "-");
        config.put("target.type", "STRING");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();
        Struct actualNested = (Struct) resStruct.get("nested");

        assertEquals(789L, actualNested.get("sub_id"));
        assertEquals(10, actualNested.get("sub_seq"));
        assertEquals("test", resStruct.get("other_field"));
        assertEquals("00789-010", resStruct.get("nested_merged"));
    }

    @Test
    public void shouldLpadAndMergeToKey() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.struct()
            .field("id_processo", Schema.INT64_SCHEMA)
            .field("seq_processo", Schema.INT32_SCHEMA)
            .build();
        Struct key = new Struct(keySchema);
        key.put("id_processo", 123L);
        key.put("seq_processo", 45);

        Schema valSchema = SchemaBuilder.struct()
            .field("some_value", Schema.STRING_SCHEMA)
            .build();
        Struct value = new Struct(valSchema);
        value.put("some_value", "original");

        SinkRecord record = new SinkRecord("test", 1, keySchema, key, valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "key.id_processo:5,key.seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "STRING");
        config.put("merged.to.key", "true");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        // Assert that the key is a new struct with a single field
        assertTrue(result.key() instanceof Struct);
        Struct resultKey = (Struct) result.key();
        assertEquals(1, resultKey.schema().fields().size());
        assertEquals("00123045", resultKey.get("process_id_seq"));

        // Assert that the merged field is also added to the value
        Struct resStruct = (Struct) result.value();
        assertEquals("original", resStruct.get("some_value"));
        assertEquals("00123045", resStruct.get("process_id_seq"));
    }

    @Test
    public void shouldHandleTombstoneWhenMergedToKey() {
        LpadMergeTransformer<SinkRecord> transformer = new LpadMergeTransformer<>();

        Schema keySchema = SchemaBuilder.struct()
            .field("id_processo", Schema.INT64_SCHEMA)
            .field("seq_processo", Schema.INT32_SCHEMA)
            .build();
        Struct key = new Struct(keySchema);
        key.put("id_processo", 123L);
        key.put("seq_processo", 45);

        // Tombstone record: value is null
        SinkRecord record = new SinkRecord("test", 1, keySchema, key, null, null, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "key.id_processo:5,key.seq_processo:3");
        config.put("merged.name", "process_id_seq");
        config.put("merged.separator", "");
        config.put("target.type", "STRING");
        config.put("merged.to.key", "true");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        // Assert that the key is a new struct with a single field
        assertTrue(result.key() instanceof Struct);
        Struct resultKey = (Struct) result.key();
        assertEquals(1, resultKey.schema().fields().size());
        assertEquals("00123045", resultKey.get("process_id_seq"));

        // Assert that the value remains null
        assertNull(result.value());
        assertNull(result.valueSchema());
    }
}