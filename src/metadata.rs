use wasm_bindgen::prelude::*;

use crate::common::properties::{Compression, Encoding};

/// Global Parquet metadata.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct ParquetMetaData(parquet::file::metadata::ParquetMetaData);

#[wasm_bindgen]
impl ParquetMetaData {
    /// Returns file metadata as reference.
    #[wasm_bindgen(js_name = fileMetadata)]
    pub fn file_metadata(&self) -> FileMetaData {
        self.0.file_metadata().clone().into()
    }

    /// Returns number of row groups in this file.
    #[wasm_bindgen(js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.0.num_row_groups()
    }

    /// Returns row group metadata for `i`th position.
    /// Position should be less than number of row groups `num_row_groups`.
    #[wasm_bindgen(js_name = rowGroup)]
    pub fn row_group(&self, i: usize) -> RowGroupMetaData {
        self.0.row_group(i).clone().into()
    }

    /// Returns row group metadata for all row groups
    #[wasm_bindgen(js_name = rowGroups)]
    pub fn row_groups(&self) -> Vec<RowGroupMetaData> {
        self.0
            .row_groups()
            .iter()
            .map(|rg| rg.clone().into())
            .collect()
    }

    // /// Returns the column index for this file if loaded
    // pub fn column_index(&self) -> Option<ParquetColumnIndex> {
    //     self.0.column_index()
    // }
}

impl From<parquet::file::metadata::ParquetMetaData> for ParquetMetaData {
    fn from(value: parquet::file::metadata::ParquetMetaData) -> Self {
        Self(value)
    }
}

impl From<ParquetMetaData> for parquet::file::metadata::ParquetMetaData {
    fn from(value: ParquetMetaData) -> Self {
        value.0
    }
}

/// Metadata for a Parquet file.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct FileMetaData(parquet::file::metadata::FileMetaData);

#[wasm_bindgen]
impl FileMetaData {
    /// Returns version of this file.
    #[wasm_bindgen]
    pub fn version(&self) -> i32 {
        self.0.version()
    }

    /// Returns number of rows in the file.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> f64 {
        self.0.num_rows() as f64
    }

    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    #[wasm_bindgen(js_name = createdBy)]
    pub fn created_by(&self) -> Option<String> {
        let s = self.0.created_by()?;
        Some(s.to_string())
    }

    /// Returns key_value_metadata of this file.
    #[wasm_bindgen(js_name = keyValueMetadata)]
    pub fn key_value_metadata(&self) -> Result<js_sys::Map, JsValue> {
        let map = js_sys::Map::new();
        if let Some(metadata) = self.0.key_value_metadata() {
            for meta in metadata {
                if let Some(value) = &meta.value {
                    map.set(&JsValue::from_str(&meta.key), &JsValue::from_str(value));
                }
            }
        }
        Ok(map)
    }
}

impl From<parquet::file::metadata::FileMetaData> for FileMetaData {
    fn from(value: parquet::file::metadata::FileMetaData) -> Self {
        Self(value)
    }
}

impl From<FileMetaData> for parquet::file::metadata::FileMetaData {
    fn from(value: FileMetaData) -> Self {
        value.0
    }
}

/// Metadata for a Parquet row group.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct RowGroupMetaData(parquet::file::metadata::RowGroupMetaData);

#[wasm_bindgen]
impl RowGroupMetaData {
    /// Number of columns in this row group.
    #[wasm_bindgen(js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// Returns column chunk metadata for `i`th column.
    #[wasm_bindgen]
    pub fn column(&self, i: usize) -> ColumnChunkMetaData {
        self.0.column(i).clone().into()
    }

    /// Returns column chunk metadata for all columns
    #[wasm_bindgen]
    pub fn columns(&self) -> Vec<ColumnChunkMetaData> {
        self.0
            .columns()
            .iter()
            .map(|col| col.clone().into())
            .collect()
    }

    /// Number of rows in this row group.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> f64 {
        self.0.num_rows() as f64
    }

    /// Total byte size of all uncompressed column data in this row group.
    #[wasm_bindgen(js_name = totalByteSize)]
    pub fn total_byte_size(&self) -> f64 {
        self.0.total_byte_size() as f64
    }

    /// Total size of all compressed column data in this row group.
    #[wasm_bindgen(js_name = compressedSize)]
    pub fn compressed_size(&self) -> f64 {
        self.0.compressed_size() as f64
    }
}

impl From<parquet::file::metadata::RowGroupMetaData> for RowGroupMetaData {
    fn from(value: parquet::file::metadata::RowGroupMetaData) -> Self {
        Self(value)
    }
}

impl From<RowGroupMetaData> for parquet::file::metadata::RowGroupMetaData {
    fn from(value: RowGroupMetaData) -> Self {
        value.0
    }
}

/// Metadata for a Parquet column chunk.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct ColumnChunkMetaData(parquet::file::metadata::ColumnChunkMetaData);

#[wasm_bindgen]
impl ColumnChunkMetaData {
    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    #[wasm_bindgen(js_name = filePath)]
    pub fn file_path(&self) -> Option<String> {
        self.0.file_path().map(|s| s.to_string())
    }

    /// Byte offset in `file_path()`.
    #[wasm_bindgen(js_name = fileOffset)]
    pub fn file_offset(&self) -> i64 {
        self.0.file_offset()
    }

    // /// Type of this column. Must be primitive.
    // pub fn column_type(&self) -> Type {
    //     self.column_descr.physical_type()
    // }

    /// Path (or identifier) of this column.
    #[wasm_bindgen(js_name = columnPath)]
    pub fn column_path(&self) -> Vec<String> {
        let path = self.0.column_path();
        path.parts().to_vec()
    }

    /// All encodings used for this column.
    #[wasm_bindgen]
    pub fn encodings(&self) -> Vec<Encoding> {
        self.0
            .encodings()
            .iter()
            .map(|encoding| (*encoding).into())
            .collect()
    }

    /// Total number of values in this column chunk.
    #[wasm_bindgen(js_name = numValues)]
    pub fn num_values(&self) -> f64 {
        self.0.num_values() as f64
    }

    /// Compression for this column.
    pub fn compression(&self) -> Compression {
        self.0.compression().into()
    }

    /// Returns the total compressed data size of this column chunk.
    #[wasm_bindgen(js_name = compressedSize)]
    pub fn compressed_size(&self) -> f64 {
        self.0.compressed_size() as f64
    }

    /// Returns the total uncompressed data size of this column chunk.
    #[wasm_bindgen(js_name = uncompressedSize)]
    pub fn uncompressed_size(&self) -> f64 {
        self.0.uncompressed_size() as f64
    }
}

impl From<parquet::file::metadata::ColumnChunkMetaData> for ColumnChunkMetaData {
    fn from(value: parquet::file::metadata::ColumnChunkMetaData) -> Self {
        Self(value)
    }
}

impl From<ColumnChunkMetaData> for parquet::file::metadata::ColumnChunkMetaData {
    fn from(value: ColumnChunkMetaData) -> Self {
        value.0
    }
}
