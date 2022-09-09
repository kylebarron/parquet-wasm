use crate::error::WasmResult;
use arrow2::io::parquet::read::infer_schema;
use parquet2::{
    schema::types::PhysicalType,
    statistics::{BooleanStatistics, PrimitiveStatistics},
};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

use super::error::ParquetWasmError;

/// Metadata for a Parquet file.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct FileMetaData(arrow2::io::parquet::read::FileMetaData);

#[wasm_bindgen]
impl FileMetaData {
    /// Clone this struct in wasm memory.
    #[wasm_bindgen]
    pub fn copy(&self) -> Self {
        FileMetaData(self.0.clone())
    }

    /// Version of this file.
    #[wasm_bindgen]
    pub fn version(&self) -> i32 {
        self.0.version
    }

    /// number of rows in the file.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.0.num_rows
    }

    /// String message for application that wrote this file.
    #[wasm_bindgen(js_name = createdBy)]
    pub fn created_by(&self) -> Option<String> {
        self.0.created_by.clone()
    }

    /// Number of row groups in the file
    #[wasm_bindgen(js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.0.row_groups.len()
    }

    /// Returns a single RowGroupMetaData by index
    #[wasm_bindgen(js_name = rowGroup)]
    pub fn row_group(&self, i: usize) -> RowGroupMetaData {
        self.0.row_groups[i].clone().into()
    }

    #[wasm_bindgen]
    pub fn schema(&self) -> SchemaDescriptor {
        SchemaDescriptor::new(self.0.schema().clone())
    }

    #[wasm_bindgen(js_name = keyValueMetadata)]
    pub fn key_value_metadata(&self) -> Result<JsValue, JsValue> {
        let mut map: HashMap<String, Option<String>> = HashMap::new();
        let metadata = &self.0.key_value_metadata;
        if let Some(metadata) = metadata {
            for item in metadata {
                map.insert(item.key.clone(), item.value.clone());
            }
        }

        match serde_wasm_bindgen::to_value(&map) {
            Ok(value) => Ok(value),
            Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
        }
    }

    // /// Column (sort) order used for `min` and `max` values of each column in this file.
    // ///
    // /// Each column order corresponds to one column, determined by its position in the
    // /// list, matching the position of the column in the schema.
    // ///
    // /// When `None` is returned, there are no column orders available, and each column
    // /// should be assumed to have undefined (legacy) column order.
    // pub fn column_order(&self, i: usize) -> ColumnOrder {
    //     let col_order = self.0.column_order(i);
    //     col_order.
    // }

    #[wasm_bindgen(js_name = arrowSchema)]
    pub fn arrow_schema(&self) -> WasmResult<crate::schema::ArrowSchema> {
        let schema = infer_schema(&self.0)?;
        Ok(schema.into())
    }
}

impl From<arrow2::io::parquet::read::FileMetaData> for FileMetaData {
    fn from(meta: arrow2::io::parquet::read::FileMetaData) -> Self {
        FileMetaData(meta)
    }
}

impl From<FileMetaData> for arrow2::io::parquet::read::FileMetaData {
    fn from(meta: FileMetaData) -> arrow2::io::parquet::read::FileMetaData {
        meta.0
    }
}

/// Metadata for a row group.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct RowGroupMetaData(arrow2::io::parquet::read::RowGroupMetaData);

#[wasm_bindgen]
impl RowGroupMetaData {
    /// Number of rows in this row group.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    /// Number of columns in this row group.
    #[wasm_bindgen(js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.columns().len()
    }

    /// Returns a single column chunk metadata by index
    #[wasm_bindgen]
    pub fn column(&self, i: usize) -> ColumnChunkMetaData {
        self.0.columns()[i].clone().into()
    }

    /// Total byte size of all uncompressed column data in this row group.
    #[wasm_bindgen(js_name = totalByteSize)]
    pub fn total_byte_size(&self) -> usize {
        self.0.total_byte_size()
    }

    /// Total size of all compressed column data in this row group.
    #[wasm_bindgen(js_name = compressedSize)]
    pub fn compressed_size(&self) -> usize {
        self.0.compressed_size()
    }
}

impl From<arrow2::io::parquet::read::RowGroupMetaData> for RowGroupMetaData {
    fn from(meta: arrow2::io::parquet::read::RowGroupMetaData) -> Self {
        RowGroupMetaData(meta)
    }
}

impl From<RowGroupMetaData> for arrow2::io::parquet::read::RowGroupMetaData {
    fn from(meta: RowGroupMetaData) -> arrow2::io::parquet::read::RowGroupMetaData {
        meta.0
    }
}

/// Metadata for a column chunk.
// This contains the `ColumnDescriptor` associated with the chunk so that deserializers have
// access to the descriptor (e.g. physical, converted, logical).
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct ColumnChunkMetaData(arrow2::io::parquet::read::ColumnChunkMetaData);

#[wasm_bindgen]
impl ColumnChunkMetaData {
    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    #[wasm_bindgen(js_name = filePath)]
    pub fn file_path(&self) -> Option<String> {
        self.0.file_path().clone()
    }

    /// Byte offset in `file_path()`.
    #[wasm_bindgen(js_name = fileOffset)]
    pub fn file_offset(&self) -> i64 {
        self.0.file_offset()
    }

    #[wasm_bindgen(js_name = pathInSchema)]
    pub fn path_in_schema(&self) -> String {
        let meta = self.0.metadata();
        let path = &meta.path_in_schema;
        path.join("/")
    }

    // /// Returns this column's [`ColumnChunk`]
    // #[wasm_bindgen(js_name = columnChunk)]
    // pub fn column_chunk(&self) -> usize {
    //     // let a = self.0.column_chunk();
    //     // let map
    //     // let val = serde_wasm_bindgen::to_value(a);

    //     // &self.column_chunk
    // }

    // /// The column's [`ColumnMetaData`]
    // #[wasm_bindgen]
    // pub fn metadata(&self) -> &ColumnMetaData {
    //     self.column_chunk.meta_data.as_ref().unwrap()
    // }

    // /// The [`ColumnDescriptor`] for this column. This descriptor contains the physical and logical type
    // /// of the pages.
    // #[wasm_bindgen]
    // pub fn descriptor(&self) -> &ColumnDescriptor {
    //     &self.column_descr
    // }

    // /// The [`PhysicalType`] of this column.
    // #[wasm_bindgen(js_name = physicalType)]
    // pub fn physical_type(&self) -> PhysicalType {
    //     self.column_descr.descriptor.primitive_type.physical_type
    // }

    pub fn statistics_exist(&self) -> WasmResult<bool> {
        Ok(self.0.statistics().is_some())
    }

    #[wasm_bindgen(js_name = getStatisticsMinValue)]
    pub fn get_statistics_min_value(&self) -> WasmResult<JsValue> {
        let statistics = self.0.statistics().unwrap()?;

        let value: JsValue = match statistics.physical_type() {
            PhysicalType::Boolean => statistics
                .as_any()
                .downcast_ref::<BooleanStatistics>()
                .unwrap()
                .min_value
                .into(),
            PhysicalType::Int32 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .min_value
                .into(),
            PhysicalType::Int64 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .min_value
                .into(),
            PhysicalType::Float => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .min_value
                .into(),
            PhysicalType::Double => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .min_value
                .into(),
            // Not implemented: Int96, ByteArray, FixedLenByteArray
            _ => {
                return Err(ParquetWasmError::InternalError(
                    "Unsupported statistics type".to_string(),
                )
                .into())
            }
        };

        Ok(value)
    }

    #[wasm_bindgen(js_name = getStatisticsMaxValue)]
    pub fn get_statistics_max_value(&self) -> WasmResult<JsValue> {
        let statistics = self.0.statistics().unwrap()?;

        let value: JsValue = match statistics.physical_type() {
            PhysicalType::Boolean => statistics
                .as_any()
                .downcast_ref::<BooleanStatistics>()
                .unwrap()
                .max_value
                .into(),
            PhysicalType::Int32 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .max_value
                .into(),
            PhysicalType::Int64 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .max_value
                .into(),
            PhysicalType::Float => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .max_value
                .into(),
            PhysicalType::Double => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .max_value
                .into(),
            // Not implemented: Int64, Int96, ByteArray, FixedLenByteArray
            _ => {
                return Err(ParquetWasmError::InternalError(
                    "Unsupported statistics type".to_string(),
                )
                .into())
            }
        };

        Ok(value)
    }

    #[wasm_bindgen(js_name = getStatisticsNullCount)]
    pub fn get_statistics_null_count(&self) -> WasmResult<JsValue> {
        let statistics = self.0.statistics().unwrap()?;

        let value: JsValue = match statistics.physical_type() {
            PhysicalType::Boolean => statistics
                .as_any()
                .downcast_ref::<BooleanStatistics>()
                .unwrap()
                .null_count
                .into(),
            PhysicalType::Int32 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .null_count
                .into(),
            PhysicalType::Int64 => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .null_count
                .into(),
            PhysicalType::Float => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .null_count
                .into(),
            PhysicalType::Double => statistics
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .null_count
                .into(),
            // Not implemented: Int64, Int96, ByteArray, FixedLenByteArray
            _ => {
                return Err(ParquetWasmError::InternalError(
                    "Unsupported statistics type".to_string(),
                )
                .into())
            }
        };

        Ok(value)
    }

    /// Total number of values in this column chunk. Note that this is not necessarily the number
    /// of rows. E.g. the (nested) array `[[1, 2], [3]]` has 2 rows and 3 values.
    #[wasm_bindgen(js_name = numValues)]
    pub fn num_values(&self) -> i64 {
        self.0.num_values()
    }

    // /// [`Compression`] for this column.
    // #[wasm_bindgen(js_name = compression)]
    // pub fn compression(&self) -> Compression {
    //     let compression = self.0.compression();
    //     compression.
    // }

    /// Returns the total compressed data size of this column chunk.
    #[wasm_bindgen(js_name = compressedSize)]
    pub fn compressed_size(&self) -> i64 {
        self.0.compressed_size()
    }

    /// Returns the total uncompressed data size of this column chunk.
    #[wasm_bindgen(js_name = uncompressedSize)]
    pub fn uncompressed_size(&self) -> i64 {
        self.0.uncompressed_size()
    }

    /// Returns the offset for the column data.
    #[wasm_bindgen(js_name = dataPageOffset)]
    pub fn data_page_offset(&self) -> i64 {
        self.0.data_page_offset()
    }

    /// Returns `true` if this column chunk contains a index page, `false` otherwise.
    #[wasm_bindgen(js_name = hasIndexPage)]
    pub fn has_index_page(&self) -> bool {
        self.0.has_index_page()
    }

    /// Returns the offset for the index page.
    #[wasm_bindgen(js_name = indexPageOffset)]
    pub fn index_page_offset(&self) -> Option<i64> {
        self.0.index_page_offset()
    }

    /// Returns the offset for the dictionary page, if any.
    #[wasm_bindgen(js_name = dictionaryPageOffset)]
    pub fn dictionary_page_offset(&self) -> Option<i64> {
        self.0.dictionary_page_offset()
    }

    /// Returns the number of encodings for this column
    #[wasm_bindgen(js_name = numColumnEncodings)]
    pub fn num_column_encodings(&self) -> usize {
        self.0.column_encoding().len()
    }

    // /// Returns the encoding for this column
    // #[wasm_bindgen(js_name = columnEncoding)]
    // pub fn column_encoding(&self, i: usize) -> Encoding {
    //     self.0.column_encoding()[i]
    // }

    /// Returns the offset and length in bytes of the column chunk within the file
    #[wasm_bindgen(js_name = byteRange)]
    pub fn byte_range(&self) -> Vec<u64> {
        let mut vec: Vec<u64> = Vec::new();
        let byte_range = self.0.byte_range();
        vec.push(byte_range.0);
        vec.push(byte_range.1);
        vec
    }
}

impl From<arrow2::io::parquet::read::ColumnChunkMetaData> for ColumnChunkMetaData {
    fn from(meta: arrow2::io::parquet::read::ColumnChunkMetaData) -> Self {
        ColumnChunkMetaData(meta)
    }
}

/// A schema descriptor. This encapsulates the top-level schemas for all the columns,
/// as well as all descriptors for all the primitive columns.
#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct SchemaDescriptor(parquet2::metadata::SchemaDescriptor);

impl SchemaDescriptor {
    pub fn new(meta: parquet2::metadata::SchemaDescriptor) -> Self {
        Self(meta)
    }
}

#[wasm_bindgen]
impl SchemaDescriptor {
    /// The schemas' name.
    #[wasm_bindgen]
    pub fn name(&self) -> String {
        self.0.name().to_string()
    }

    /// The number of columns in the schema
    #[wasm_bindgen(js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.columns().len()
    }

    // /// The [`ColumnDescriptor`] (leafs) of this schema.
    // ///
    // /// Note that, for nested fields, this may contain more entries than the number of fields
    // /// in the file - e.g. a struct field may have two columns.
    // pub fn column(&self, i: usize) -> ColumnDescriptor {
    //     ColumnDescriptor::new(self.0.columns()[i])
    // }

    /// The number of fields in the schema
    #[wasm_bindgen(js_name = numFields)]
    pub fn num_fields(&self) -> usize {
        self.0.fields().len()
    }

    // /// The schemas' fields.
    // #[wasm_bindgen]
    // pub fn fields(&self, i: usize) -> ParquetType {
    //     ParquetType::new(self.0.fields()[i])
    // }
}
