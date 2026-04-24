use parquet::data_type::AsBytes;
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

    /// Returns the column's page index from this file if available.
    ///
    /// The page index is useful for finding regions to select with `offset` and
    /// `limit` on `ReaderOptions` when searching within a sorted column.
    #[wasm_bindgen(js_name = columnIndexFor, unchecked_return_type="DataPage[]")]
    pub fn column_index_for(
        &self,
        column: usize,
    ) -> Result<Vec<JsValue>, serde_wasm_bindgen::Error> {
        let col_indices = match self.0.column_index() {
            Some(x) => x,
            None => return Ok(Default::default()),
        };
        let offset_indices = match self.0.offset_index() {
            Some(x) => x,
            None => return Ok(Default::default()),
        };
        let mut pages_acc: Vec<JsValue> = Vec::new();
        let mut total_rows = 0;
        for ((rg_i, col_indices_rg), offset_indices_rg) in col_indices.iter().enumerate().zip(offset_indices.iter()) {
            let col = col_indices_rg.get(column);
            let offset = offset_indices_rg.get(column);
            let rg_num_rows = self.0.row_group(rg_i).num_rows();
            match (col, offset) {
                (Some(col), Some(offset)) => match col {
                    parquet::file::page_index::index::Index::NONE => continue,
                    parquet::file::page_index::index::Index::BOOLEAN(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min,
                                page.max,
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::INT32(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min,
                                page.max,
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::INT64(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min,
                                page.max,
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::FLOAT(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min,
                                page.max,
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::DOUBLE(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min,
                                page.max,
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::INT96(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min().map(|v| v.to_nanos()),
                                page.max().map(|v| v.to_nanos()),
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    },
                    parquet::file::page_index::index::Index::BYTE_ARRAY(native_index) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min().map(|v| v.data().to_vec()),
                                page.max().map(|v| v.data().to_vec()),
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    }
                    parquet::file::page_index::index::Index::FIXED_LEN_BYTE_ARRAY(
                        native_index,
                    ) => {
                        for (pg_i, (page, loc)) in native_index.indexes.iter().zip(offset.page_locations()).enumerate()
                        {
                            let end_row =
                            if let Some(next_loc) = offset.page_locations().get(pg_i + 1) {
                                next_loc.first_row_index + total_rows
                            } else {
                                rg_num_rows + total_rows
                            };
                            pages_acc.push(serde_wasm_bindgen::to_value(&Page::new(
                                page.min().map(|v| v.data().to_vec()),
                                page.max().map(|v| v.data().to_vec()),
                                loc.first_row_index,
                                page.null_count(),
                                rg_i,
                                end_row
                            ))?);
                        }
                    },
                },
                (_, _) => {
                    continue;
                }
            }

            total_rows += rg_num_rows;
        }

        Ok(pages_acc)
    }
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

    /// Read the row-group-level statistics for this column, if available.
    ///
    /// This is useful for checking if a row group is worth visiting if you
    /// are searching for a value in a sorted column.
    #[wasm_bindgen(unchecked_return_type = "ColumnChunkStatistic | null")]
    pub fn statistics(&self) -> wasm_bindgen::JsValue {
        let v = if let Some(stat) = self.0.statistics() {
            match stat {
                parquet::file::statistics::Statistics::Boolean(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied(),
                        value_statistics.max_opt().copied(),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::Int32(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied(),
                        value_statistics.max_opt().copied(),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::Int64(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied(),
                        value_statistics.max_opt().copied(),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::Int96(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied().map(|v| v.to_seconds()),
                        value_statistics.max_opt().copied().map(|v| v.to_seconds()),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::Float(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied(),
                        value_statistics.max_opt().copied(),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::Double(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().copied(),
                        value_statistics.max_opt().copied(),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::ByteArray(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().map(|v| v.as_bytes().to_vec()),
                        value_statistics.max_opt().map(|v| v.as_bytes().to_vec()),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
                parquet::file::statistics::Statistics::FixedLenByteArray(value_statistics) => {
                    serde_wasm_bindgen::to_value(&JsStatistics::new(
                        value_statistics.min_opt().map(|v| v.as_bytes().to_vec()),
                        value_statistics.max_opt().map(|v| v.as_bytes().to_vec()),
                        value_statistics.distinct_count(),
                        value_statistics.null_count_opt(),
                        value_statistics.max_is_exact(),
                        value_statistics.min_is_exact(),
                    ))
                    .ok()
                }
            }
        } else {
            None
        };
        v.unwrap_or_else(|| JsValue::null())
    }
}

#[derive(serde::Serialize)]
pub struct JsStatistics<T: serde::Serialize> {
    pub min_value: Option<T>,
    pub max_value: Option<T>,
    // Distinct count could be omitted in some cases
    pub distinct_count: Option<u64>,
    pub null_count: Option<u64>,

    // Whether or not the min or max values are exact, or truncated.
    pub is_max_value_exact: bool,
    pub is_min_value_exact: bool,
}

impl<T: serde::Serialize> JsStatistics<T> {
    pub fn new(
        min_value: Option<T>,
        max_value: Option<T>,
        distinct_count: Option<u64>,
        null_count: Option<u64>,
        is_max_value_exact: bool,
        is_min_value_exact: bool,
    ) -> Self {
        Self {
            min_value,
            max_value,
            distinct_count,
            null_count,
            is_max_value_exact,
            is_min_value_exact,
        }
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

#[derive(Debug, serde::Serialize)]
pub struct Page<T: serde::Serialize> {
    pub min: Option<T>,
    pub max: Option<T>,
    pub start_row: i64,
    pub null_count: Option<i64>,
    pub row_group_index: usize,
    pub end_row: i64,
}

impl<T: serde::Serialize> Page<T> {
    pub fn new(min: Option<T>, max: Option<T>, start_row: i64, null_count: Option<i64>, row_group_index: usize, end_row: i64) -> Self {
        Self {
            min,
            max,
            start_row,
            null_count,
            row_group_index,
            end_row
        }
    }
}


#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"

export interface ColumnChunkStatistic {
    min_value: any | null,
    max_value: any | null,
    // Distinct count could be omitted in some cases
    distinct_count: number | null,
    null_count: number | null,

    // Whether or not the min or max values are exact, or truncated.
    is_max_value_exact: boolean,
    is_min_value_exact: boolean,
}

export interface DataPage {
    min: any | null,
    max: any | null,
    start_row: number,
    row_group_index: number,
    end_row: number,
    null_count: number | null,
};
"#;