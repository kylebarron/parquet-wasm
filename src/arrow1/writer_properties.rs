use crate::common::writer_properties::{Compression, Encoding, WriterVersion};
use wasm_bindgen::prelude::*;

impl From<Encoding> for parquet::basic::Encoding {
    fn from(x: Encoding) -> parquet::basic::Encoding {
        match x {
            Encoding::PLAIN => parquet::basic::Encoding::PLAIN,
            Encoding::PLAIN_DICTIONARY => parquet::basic::Encoding::PLAIN_DICTIONARY,
            Encoding::RLE => parquet::basic::Encoding::RLE,
            Encoding::BIT_PACKED => parquet::basic::Encoding::BIT_PACKED,
            Encoding::DELTA_BINARY_PACKED => parquet::basic::Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_LENGTH_BYTE_ARRAY => parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY => parquet::basic::Encoding::DELTA_BYTE_ARRAY,
            Encoding::RLE_DICTIONARY => parquet::basic::Encoding::RLE_DICTIONARY,
            Encoding::BYTE_STREAM_SPLIT => parquet::basic::Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

impl From<Compression> for parquet::basic::Compression {
    fn from(x: Compression) -> parquet::basic::Compression {
        match x {
            Compression::UNCOMPRESSED => parquet::basic::Compression::UNCOMPRESSED,
            Compression::SNAPPY => parquet::basic::Compression::SNAPPY,
            Compression::GZIP => parquet::basic::Compression::GZIP(Default::default()),
            Compression::BROTLI => parquet::basic::Compression::BROTLI(Default::default()),
            Compression::LZ4 => parquet::basic::Compression::LZ4,
            Compression::ZSTD => parquet::basic::Compression::ZSTD(Default::default()),
            // TODO: fix this. Though LZ4 isn't supported in arrow1 for wasm anyways
            Compression::LZ4_RAW => parquet::basic::Compression::LZ4,
        }
    }
}

impl From<WriterVersion> for parquet::file::properties::WriterVersion {
    fn from(x: WriterVersion) -> parquet::file::properties::WriterVersion {
        match x {
            WriterVersion::V1 => parquet::file::properties::WriterVersion::PARQUET_1_0,
            WriterVersion::V2 => parquet::file::properties::WriterVersion::PARQUET_2_0,
        }
    }
}

/// Controls the level of statistics to be computed by the writer
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[wasm_bindgen]
pub enum EnabledStatistics {
    /// Compute no statistics
    None,
    /// Compute chunk-level statistics but not page-level
    Chunk,
    /// Compute page-level and chunk-level statistics
    Page,
}

impl From<EnabledStatistics> for parquet::file::properties::EnabledStatistics {
    fn from(statistics: EnabledStatistics) -> Self {
        match statistics {
            EnabledStatistics::None => parquet::file::properties::EnabledStatistics::None,
            EnabledStatistics::Chunk => parquet::file::properties::EnabledStatistics::Chunk,
            EnabledStatistics::Page => parquet::file::properties::EnabledStatistics::Page,
        }
    }
}

/// Immutable struct to hold writing configuration for `writeParquet`.
///
/// Use {@linkcode WriterPropertiesBuilder} to create a configuration, then call {@linkcode
/// WriterPropertiesBuilder.build} to create an instance of `WriterProperties`.
#[wasm_bindgen]
pub struct WriterProperties(parquet::file::properties::WriterProperties);

impl From<WriterProperties> for parquet::file::properties::WriterProperties {
    fn from(props: WriterProperties) -> Self {
        props.0
    }
}

/// Builder to create a writing configuration for `writeParquet`
///
/// Call {@linkcode build} on the finished builder to create an immputable {@linkcode WriterProperties} to pass to `writeParquet`
#[wasm_bindgen]
pub struct WriterPropertiesBuilder(parquet::file::properties::WriterPropertiesBuilder);

#[wasm_bindgen]
impl WriterPropertiesBuilder {
    /// Returns default state of the builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder(parquet::file::properties::WriterProperties::builder())
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    #[wasm_bindgen]
    pub fn build(self) -> WriterProperties {
        WriterProperties(self.0.build())
    }

    // ----------------------------------------------------------------------
    // Writer properties related to a file

    /// Sets writer version.
    #[wasm_bindgen(js_name = setWriterVersion)]
    pub fn set_writer_version(self, value: WriterVersion) -> Self {
        Self(self.0.set_writer_version(value.into()))
    }

    /// Sets data page size limit.
    #[wasm_bindgen(js_name = setDataPageSizeLimit)]
    pub fn set_data_page_size_limit(self, value: usize) -> Self {
        Self(self.0.set_data_page_size_limit(value))
    }

    /// Sets dictionary page size limit.
    #[wasm_bindgen(js_name = setDictionaryPageSizeLimit)]
    pub fn set_dictionary_page_size_limit(self, value: usize) -> Self {
        Self(self.0.set_dictionary_page_size_limit(value))
    }

    /// Sets write batch size.
    #[wasm_bindgen(js_name = setWriteBatchSize)]
    pub fn set_write_batch_size(self, value: usize) -> Self {
        Self(self.0.set_write_batch_size(value))
    }

    /// Sets maximum number of rows in a row group.
    #[wasm_bindgen(js_name = setMaxRowGroupSize)]
    pub fn set_max_row_group_size(self, value: usize) -> Self {
        Self(self.0.set_max_row_group_size(value))
    }

    /// Sets "created by" property.
    #[wasm_bindgen(js_name = setCreatedBy)]
    pub fn set_created_by(self, value: String) -> Self {
        Self(self.0.set_created_by(value))
    }

    // /// Sets "key_value_metadata" property.
    // #[wasm_bindgen(js_name = setKeyValueMetadata)]
    // pub fn set_key_value_metadata(
    //     self,
    //     value: Option<Vec<parquet::file::metadata::KeyValue>>,
    // ) -> Self {
    //     Self {
    //         0: self.0.set_key_value_metadata(value),
    //     }
    // }

    // ----------------------------------------------------------------------
    // Setters for any column (global)

    /// Sets encoding for any column.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for all
    /// columns. In case when dictionary is enabled for any column, this value is
    /// considered to be a fallback encoding for that column.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set.
    #[wasm_bindgen(js_name = setEncoding)]
    pub fn set_encoding(self, value: Encoding) -> Self {
        Self(self.0.set_encoding(value.into()))
    }

    /// Sets compression codec for any column.
    #[wasm_bindgen(js_name = setCompression)]
    pub fn set_compression(self, value: Compression) -> Self {
        Self(self.0.set_compression(value.into()))
    }

    /// Sets flag to enable/disable dictionary encoding for any column.
    ///
    /// Use this method to set dictionary encoding, instead of explicitly specifying
    /// encoding in `set_encoding` method.
    #[wasm_bindgen(js_name = setDictionaryEnabled)]
    pub fn set_dictionary_enabled(self, value: bool) -> Self {
        Self(self.0.set_dictionary_enabled(value))
    }

    /// Sets flag to enable/disable statistics for any column.
    #[wasm_bindgen(js_name = setStatisticsEnabled)]
    pub fn set_statistics_enabled(self, value: EnabledStatistics) -> Self {
        Self(self.0.set_statistics_enabled(value.into()))
    }

    /// Sets max statistics size for any column.
    /// Applicable only if statistics are enabled.
    #[wasm_bindgen(js_name = setMaxStatisticsSize)]
    pub fn set_max_statistics_size(self, value: usize) -> Self {
        Self(self.0.set_max_statistics_size(value))
    }

    // ----------------------------------------------------------------------
    // Setters for a specific column

    /// Sets encoding for a column.
    /// Takes precedence over globally defined settings.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for this
    /// column. In case when dictionary is enabled for this column, either through
    /// global defaults or explicitly, this value is considered to be a fallback
    /// encoding for this column.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set.
    #[wasm_bindgen(js_name = setColumnEncoding)]
    pub fn set_column_encoding(self, col: String, value: Encoding) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self(self.0.set_column_encoding(column_path, value.into()))
    }

    /// Sets compression codec for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnCompression)]
    pub fn set_column_compression(self, col: String, value: Compression) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self(self.0.set_column_compression(column_path, value.into()))
    }

    /// Sets flag to enable/disable dictionary encoding for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnDictionaryEnabled)]
    pub fn set_column_dictionary_enabled(self, col: String, value: bool) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self(self.0.set_column_dictionary_enabled(column_path, value))
    }

    /// Sets flag to enable/disable statistics for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnStatisticsEnabled)]
    pub fn set_column_statistics_enabled(self, col: String, value: EnabledStatistics) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self(
            self.0
                .set_column_statistics_enabled(column_path, value.into()),
        )
    }

    /// Sets max size for statistics for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnMaxStatisticsSize)]
    pub fn set_column_max_statistics_size(self, col: String, value: usize) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self(self.0.set_column_max_statistics_size(column_path, value))
    }
}

impl Default for WriterPropertiesBuilder {
    fn default() -> Self {
        WriterPropertiesBuilder::new()
    }
}
