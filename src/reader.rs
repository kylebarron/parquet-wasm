use std::sync::Arc;

use crate::error::Result;
use crate::read_options::JsReaderOptions;
use arrow_schema::{DataType, FieldRef};
use arrow_wasm::{Schema, Table};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
pub fn read_parquet(parquet_file: Vec<u8>, options: JsReaderOptions) -> Result<Table> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();

    let metadata = ArrowReaderMetadata::load(&cursor, Default::default())?;
    let metadata = cast_metadata_view_types(metadata)?;

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(cursor, metadata);

    let schema = builder.schema().clone();

    if let Some(batch_size) = options.batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    if let Some(row_groups) = options.row_groups {
        builder = builder.with_row_groups(row_groups);
    }

    if let Some(limit) = options.limit {
        builder = builder.with_limit(limit);
    }

    if let Some(offset) = options.offset {
        builder = builder.with_offset(offset);
    }

    // Create Arrow reader
    let reader = builder.build()?;

    let mut batches = vec![];

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(schema, batches))
}

/// Internal function to read a buffer with Parquet data into an Arrow schema
pub fn read_schema(parquet_file: Vec<u8>) -> Result<Schema> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let schema = builder.schema().clone();
    Ok(schema.into())
}

/// Cast any view types in the metadata's schema to non-view types
pub(crate) fn cast_metadata_view_types(
    metadata: ArrowReaderMetadata,
) -> Result<ArrowReaderMetadata> {
    let original_arrow_schema = metadata.schema();
    if has_view_types(original_arrow_schema.fields().iter()) {
        let new_schema = cast_view_types(original_arrow_schema);
        let arrow_options = ArrowReaderOptions::default().with_schema(new_schema);
        Ok(ArrowReaderMetadata::try_new(
            metadata.metadata().clone(),
            arrow_options,
        )?)
    } else {
        Ok(metadata)
    }
}

/// Cast any view types in the schema to non-view types
///
/// Casts:
///
/// - StringView to String
/// - BinaryView to Binary
///
/// Arrow JS does not currently support view types
/// https://github.com/apache/arrow-js/issues/44
fn cast_view_types(schema: &arrow_schema::Schema) -> arrow_schema::SchemaRef {
    let new_fields = _cast_view_types_of_fields(schema.fields().iter());
    Arc::new(arrow_schema::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

/// Recursively cast any view types in the fields to non-view types
///
/// This includes any view types that are the children of nested types like Structs and Lists
fn _cast_view_types_of_fields<'a>(fields: impl Iterator<Item = &'a FieldRef>) -> Vec<FieldRef> {
    fields
        .map(|field| {
            let new_data_type = match field.data_type() {
                DataType::Utf8View => DataType::Utf8,
                DataType::BinaryView => DataType::Binary,
                DataType::Struct(struct_fields) => {
                    DataType::Struct(_cast_view_types_of_fields(struct_fields.iter()).into())
                }
                DataType::List(inner_field) => DataType::List(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                ),
                DataType::LargeList(inner_field) => DataType::LargeList(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                ),
                DataType::FixedSizeList(inner_field, list_size) => DataType::FixedSizeList(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                    *list_size,
                ),
                other => other.clone(),
            };
            Arc::new(field.as_ref().clone().with_data_type(new_data_type))
        })
        .collect()
}

fn has_view_types<'a>(mut fields: impl Iterator<Item = &'a FieldRef>) -> bool {
    fields.any(|field| match field.data_type() {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(struct_fields) => has_view_types(struct_fields.iter()),
        DataType::List(inner_field) => has_view_types([inner_field].into_iter()),
        DataType::LargeList(inner_field) => has_view_types([inner_field].into_iter()),
        DataType::FixedSizeList(inner_field, _list_size) => {
            has_view_types([inner_field].into_iter())
        }
        _other => false,
    })
}
