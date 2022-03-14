use wasm_bindgen::prelude::*;

/// Encodings supported by Parquet.
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[wasm_bindgen]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    PLAIN,

    /// **Deprecated** dictionary encoding.
    ///
    /// The values in the dictionary are encoded using PLAIN encoding.
    /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
    /// PLAIN encoding is used for dictionary page.
    PLAIN_DICTIONARY,

    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    RLE,

    /// Bit packed encoding.
    ///
    /// This can only be used if the data has a known max width.
    /// Usable for definition/repetition levels encoding.
    BIT_PACKED,

    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DELTA_BINARY_PACKED,

    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
    DELTA_LENGTH_BYTE_ARRAY,

    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DELTA_BYTE_ARRAY,

    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RLE_DICTIONARY,

    /// Encoding for floating-point data.
    ///
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of an FP value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards.
    BYTE_STREAM_SPLIT,
}

impl Encoding {
    pub fn to_upstream(self) -> parquet::basic::Encoding {
        match self {
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

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(non_camel_case_types)]
#[wasm_bindgen]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
}

impl Compression {
    pub fn to_upstream(self) -> parquet::basic::Compression {
        match self {
            Compression::UNCOMPRESSED => parquet::basic::Compression::UNCOMPRESSED,
            Compression::SNAPPY => parquet::basic::Compression::SNAPPY,
            Compression::GZIP => parquet::basic::Compression::GZIP,
            Compression::LZO => parquet::basic::Compression::LZO,
            Compression::BROTLI => parquet::basic::Compression::BROTLI,
            Compression::LZ4 => parquet::basic::Compression::LZ4,
            Compression::ZSTD => parquet::basic::Compression::ZSTD,
        }
    }
}

#[allow(non_camel_case_types)]
#[wasm_bindgen]
pub enum WriterVersion {
    PARQUET_1_0,
    PARQUET_2_0,
}

impl WriterVersion {
    pub fn to_upstream(self) -> parquet::file::properties::WriterVersion {
        match self {
            WriterVersion::PARQUET_1_0 => parquet::file::properties::WriterVersion::PARQUET_1_0,
            WriterVersion::PARQUET_2_0 => parquet::file::properties::WriterVersion::PARQUET_2_0,
        }
    }
}

#[wasm_bindgen]
pub struct WriterProperties(parquet::file::properties::WriterProperties);

impl WriterProperties {
    pub fn to_upstream(self) -> parquet::file::properties::WriterProperties {
        self.0
    }
}

#[wasm_bindgen]
pub struct WriterPropertiesBuilder(parquet::file::properties::WriterPropertiesBuilder);

#[wasm_bindgen]
impl WriterPropertiesBuilder {
    /// Returns default state of the builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder {
            0: parquet::file::properties::WriterProperties::builder(),
        }
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    #[wasm_bindgen]
    pub fn build(self) -> WriterProperties {
        WriterProperties { 0: self.0.build() }
    }

    // ----------------------------------------------------------------------
    // Writer properties related to a file

    /// Sets writer version.
    #[wasm_bindgen(js_name = setWriterVersion)]
    pub fn set_writer_version(self, value: WriterVersion) -> Self {
        Self {
            0: self.0.set_writer_version(value.to_upstream()),
        }
    }

    /// Sets data page size limit.
    #[wasm_bindgen(js_name = setDataPagesizeLimit)]
    pub fn set_data_pagesize_limit(self, value: usize) -> Self {
        Self {
            0: self.0.set_data_pagesize_limit(value),
        }
    }

    /// Sets dictionary page size limit.
    #[wasm_bindgen(js_name = setDictionaryPagesizeLimit)]
    pub fn set_dictionary_pagesize_limit(self, value: usize) -> Self {
        Self {
            0: self.0.set_dictionary_pagesize_limit(value),
        }
    }

    /// Sets write batch size.
    #[wasm_bindgen(js_name = setWriteBatchSize)]
    pub fn set_write_batch_size(self, value: usize) -> Self {
        Self {
            0: self.0.set_write_batch_size(value),
        }
    }

    /// Sets maximum number of rows in a row group.
    #[wasm_bindgen(js_name = setMaxRowGroupSize)]
    pub fn set_max_row_group_size(self, value: usize) -> Self {
        Self {
            0: self.0.set_max_row_group_size(value),
        }
    }

    /// Sets "created by" property.
    #[wasm_bindgen(js_name = setCreatedBy)]
    pub fn set_created_by(self, value: String) -> Self {
        Self {
            0: self.0.set_created_by(value),
        }
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
        Self {
            0: self.0.set_encoding(value.to_upstream()),
        }
    }

    /// Sets compression codec for any column.
    #[wasm_bindgen(js_name = setCompression)]
    pub fn set_compression(self, value: Compression) -> Self {
        Self {
            0: self.0.set_compression(value.to_upstream()),
        }
    }

    /// Sets flag to enable/disable dictionary encoding for any column.
    ///
    /// Use this method to set dictionary encoding, instead of explicitly specifying
    /// encoding in `set_encoding` method.
    #[wasm_bindgen(js_name = setDictionaryEnabled)]
    pub fn set_dictionary_enabled(self, value: bool) -> Self {
        Self {
            0: self.0.set_dictionary_enabled(value),
        }
    }

    /// Sets flag to enable/disable statistics for any column.
    #[wasm_bindgen(js_name = setStatisticsEnabled)]
    pub fn set_statistics_enabled(self, value: bool) -> Self {
        Self {
            0: self.0.set_statistics_enabled(value),
        }
    }

    /// Sets max statistics size for any column.
    /// Applicable only if statistics are enabled.
    #[wasm_bindgen(js_name = setMaxStatisticsSize)]
    pub fn set_max_statistics_size(self, value: usize) -> Self {
        Self {
            0: self.0.set_max_statistics_size(value),
        }
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
        Self {
            0: self.0.set_column_encoding(column_path, value.to_upstream()),
        }
    }

    /// Sets compression codec for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnCompression)]
    pub fn set_column_compression(self, col: String, value: Compression) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self {
            0: self
                .0
                .set_column_compression(column_path, value.to_upstream()),
        }
    }

    /// Sets flag to enable/disable dictionary encoding for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnDictionaryEnabled)]
    pub fn set_column_dictionary_enabled(self, col: String, value: bool) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self {
            0: self.0.set_column_dictionary_enabled(column_path, value),
        }
    }

    /// Sets flag to enable/disable statistics for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnStatisticsEnabled)]
    pub fn set_column_statistics_enabled(self, col: String, value: bool) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self {
            0: self.0.set_column_statistics_enabled(column_path, value),
        }
    }

    /// Sets max size for statistics for a column.
    /// Takes precedence over globally defined settings.
    #[wasm_bindgen(js_name = setColumnMaxStatisticsSize)]
    pub fn set_column_max_statistics_size(self, col: String, value: usize) -> Self {
        let column_path = parquet::schema::types::ColumnPath::from(col);
        Self {
            0: self.0.set_column_max_statistics_size(column_path, value),
        }
    }
}

impl WriterPropertiesBuilder {
    pub fn new_from_rust() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder {
            0: parquet::file::properties::WriterProperties::builder(),
        }
    }
}
