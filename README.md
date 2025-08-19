# Kafka Connect LPAD Merge Transformer

A Kafka Connect Single Message Transform (SMT) to concatenate multiple fields, apply left-padding with zeros (LPAD) to individual fields, and optionally convert the final merged string to a specific numeric type (INT64, FLOAT64, DECIMAL).

## Features

*   **LPAD Functionality:** Left-pads specified fields with zeros to a desired length.
*   **Field Concatenation:** Merges multiple fields into a single new field.
*   **Key and Value Support:** Can access fields from both the record key (using `key.field_name`) and the record value.
*   **Type Conversion:** Converts the concatenated string to `STRING`, `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, or `DECIMAL`.
*   **Configurable Separator:** Allows specifying a separator between concatenated field values.
*   **Nested Field Support:** Can process fields nested within `Struct` or `Map` types using dot notation (e.g., `nested.field`).
*   **Schemaless and Schema-based Support:** Works with both schemaless (Map) and schema-based (Struct) records.
*   **Error Handling:** Throws an error if the merged value cannot be converted to the target type.
*   **Tombstone Handling:** Correctly handles tombstone events, especially when updating the key.

## Installation

1.  Download the latest release from the [GitHub Releases](https://github.com/leochaves/kafka-connect-lpad-merge-transformer/releases) page.
2.  Extract the downloaded archive and copy the `kafka-connect-lpad-merge-transformer-1.0.0.jar` file to the `plugin.path` of your Kafka Connect cluster.
3.  Restart your Kafka Connect workers.

## Configuration

The `LpadMergeTransformer` supports the following configuration properties:

| Property                 | Type    | Default       | Importance | Description                                                                                             |
| :----------------------- | :------ | :------------ | :--------- | :------------------------------------------------------------------------------------------------------ |
| `fields`                 | `STRING`| (No Default)  | `HIGH`     | Comma-separated list of `field_name:length` pairs to concatenate. Use `key.field_name` to access fields in the record key, or dot notation for nested value fields (e.g., `parent.child`). If `length` is omitted (e.g., `field1`), no LPAD is applied to that field. |
| `merged.name`            | `STRING`| (No Default)  | `HIGH`     | Name of the new field where the concatenated value will be stored.                                      |
| `merged.separator`       | `STRING`| `""`          | `LOW`      | Separator string to use between concatenated field values. Defaults to no separator.                    |
| `target.type`            | `STRING`| `"STRING"`    | `MEDIUM`   | Target Kafka Connect `Schema.Type` for the merged field. Supported values: `STRING`, `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `DECIMAL`. |
| `decimal.scale`          | `INT`   | `0`           | `LOW`      | Scale for `DECIMAL` type, if `target.type` is `DECIMAL`.                                                |
| `merged.to.key`          | `BOOLEAN`| `false`       | `MEDIUM`   | If `true`, sets the merged value as the new record key. This also makes the transform handle tombstone events correctly. |

## Usage Examples

### 1. LPAD and Merge to String

```json
{
  "transforms": "lpadMerge",
  "transforms.lpadMerge.type": "com.tjmg.kafka.connect.lpadmerge.LpadMergeTransformer",
  "transforms.lpadMerge.fields": "id_processo:30,seq_processo:10",
  "transforms.lpadMerge.merged.name": "process_id_seq",
  "transforms.lpadMerge.target.type": "STRING"
}
```

### 2. Merging Fields from Record Key and Value and setting as Key

```json
{
  "transforms": "lpadMerge",
  "transforms.lpadMerge.type": "com.tjmg.kafka.connect.lpadmerge.LpadMergeTransformer",
  "transforms.lpadMerge.fields": "key.tenant_id:4,user_id:8",
  "transforms.lpadMerge.merged.name": "global_user_id",
  "transforms.lpadMerge.merged.separator": "-",
  "transforms.lpadMerge.merged.to.key": "true"
}
```

### 3. LPAD and Merge to INT64

```json
{
  "transforms": "lpadMerge",
  "transforms.lpadMerge.type": "com.tjmg.kafka.connect.lpadmerge.LpadMergeTransformer",
  "transforms.lpadMerge.fields": "id_processo:5,seq_processo:3",
  "transforms.lpadMerge.merged.name": "process_id_seq",
  "transforms.lpadMerge.target.type": "INT64"
}
```

### 4. LPAD and Merge to DECIMAL

```json
{
  "transforms": "lpadMerge",
  "transforms.lpadMerge.type": "com.tjmg.kafka.connect.lpadmerge.LpadMergeTransformer",
  "transforms.lpadMerge.fields": "amount:10",
  "transforms.lpadMerge.merged.name": "formatted_amount",
  "transforms.lpadMerge.target.type": "DECIMAL",
  "transforms.lpadMerge.decimal.scale": "2"
}
```

### 5. Handling Nested Fields

```json
{
  "transforms": "lpadMerge",
  "transforms.lpadMerge.type": "com.tjmg.kafka.connect.lpadmerge.LpadMergeTransformer",
  "transforms.lpadMerge.fields": "nested.sub_id:5,nested.sub_seq:3",
  "transforms.lpadMerge.merged.name": "nested_merged",
  "transforms.lpadMerge.merged.separator": "-"
}
```

## Building from Source

To build the project from source, you need:

*   Java 8
*   Maven

```bash
git clone https://github.com/your-github-username/kafka-connect-lpad-merge-transformer.git
cd kafka-connect-lpad-merge-transformer
mvn clean package
```

## Development

To run the tests, execute the following command:

```bash
mvn test
```

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
