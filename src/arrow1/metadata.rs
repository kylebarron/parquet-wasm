use parquet::file::metadata;
use wasm_bindgen::prelude::*;

/// Global Parquet metadata.
#[wasm_bindgen]
pub struct ParquetMetadata(metadata::ParquetMetaData);

impl ParquetMetadata {
    pub fn new(meta: metadata::ParquetMetaData) -> Self {
        Self(meta)
    }
}

#[wasm_bindgen]
impl ParquetMetadata {
    /// Returns file metadata
    #[wasm_bindgen(js_name = fileMetaData)]
    pub fn file_metadata(self) -> FileMetadata {
        FileMetadata::new(self.0.file_metadata().clone())
    }

    /// Returns number of row groups in this file.
    #[wasm_bindgen(js_name = numRowGroups)]
    pub fn num_row_groups(self) -> usize {
        self.0.num_row_groups()
    }

    /// Returns row group metadata for `i`th position. Position should be less than number of row
    /// groups `num_row_groups`.
    #[wasm_bindgen(js_name = rowGroup)]
    pub fn row_group(self, i: usize) -> RowGroupMetadata {
        RowGroupMetadata::new(self.0.row_group(i).clone())
    }
}

/// Metadata for a Parquet file.
#[wasm_bindgen]
pub struct FileMetadata(metadata::FileMetaData);

impl FileMetadata {
    pub fn new(meta: metadata::FileMetaData) -> Self {
        Self(meta)
    }
}

#[wasm_bindgen]
impl FileMetadata {
    /// Returns version of this file.
    #[wasm_bindgen]
    pub fn version(&self) -> i32 {
        self.0.version()
    }

    /// Returns number of rows in the file.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> Option<u32> {
        // Cast to u32 to not use JS bigint
        // Can revisit in the future
        u32::try_from(self.0.num_rows()).ok()
    }

    /// String message for application that wrote this file.
    #[wasm_bindgen(js_name = createdBy)]
    pub fn created_by(&self) -> Option<String> {
        self.0.created_by().clone()
    }

    // #[wasm_bindgen(js_name = keyValueMetadata)]
    // pub fn key_value_metadata(&self) -> {
    //     self.0.key_value_metadata()
    // }
}

/// Metadata for a row group
#[wasm_bindgen]
pub struct RowGroupMetadata(metadata::RowGroupMetaData);

impl RowGroupMetadata {
    pub fn new(meta: metadata::RowGroupMetaData) -> Self {
        Self(meta)
    }
}

#[wasm_bindgen]
impl RowGroupMetadata {
    /// Number of columns in this row group.
    #[wasm_bindgen(js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// Returns column chunk metadata for `i`th column.
    #[wasm_bindgen]
    pub fn column(&self, i: usize) -> ColumnChunkMetadata {
        ColumnChunkMetadata::new(self.0.column(i).clone())
    }

    /// Number of rows in this row group.
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows(&self) -> Option<u32> {
        // Cast to u32 to not use JS bigint
        // Can revisit in the future
        u32::try_from(self.0.num_rows()).ok()
    }

    /// Total byte size of all uncompressed column data in this row group.
    #[wasm_bindgen(js_name = totalByteSize)]
    pub fn total_byte_size(&self) -> Option<u32> {
        // Cast to u32 to not use JS bigint
        // Can revisit in the future
        u32::try_from(self.0.total_byte_size()).ok()
    }

    /// Total size of all compressed column data in this row group.
    #[wasm_bindgen(js_name = compressedSize)]
    pub fn compressed_size(&self) -> Option<u32> {
        // Cast to u32 to not use JS bigint
        // Can revisit in the future
        u32::try_from(self.0.compressed_size()).ok()
    }
}

/// Metadata for a column chunk.
#[wasm_bindgen]
pub struct ColumnChunkMetadata(metadata::ColumnChunkMetaData);

impl ColumnChunkMetadata {
    pub fn new(meta: metadata::ColumnChunkMetaData) -> Self {
        Self(meta)
    }
}

#[wasm_bindgen]
impl ColumnChunkMetadata {
    // Column chunk metadata methods not yet implemented
}
