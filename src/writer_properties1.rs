extern crate web_sys;
// #[cfg(feature = "arrow1")]

use parquet::file::properties;
use wasm_bindgen::prelude::*;

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[cfg(target_arch = "wasm32")]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! log {
    ( $( $t:tt )* ) => {
        println!("LOG - {}", format!( $( $t )* ));
    }
}

#[wasm_bindgen]
pub enum WriterVersion {
    PARQUET_1_0,
    PARQUET_2_0,
}

#[wasm_bindgen]
pub struct WriterProperties(parquet::file::properties::WriterProperties);

#[wasm_bindgen]
pub struct WriterPropertiesBuilder(parquet::file::properties::WriterPropertiesBuilder);

#[wasm_bindgen]
impl WriterPropertiesBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder {
            0: properties::WriterProperties::builder(),
        }
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    pub fn build(self) -> WriterProperties {
        let props = self.0.build();
        log!("{:?}", props);
        WriterProperties { 0: props }
    }

    /// Sets writer version.
    #[wasm_bindgen(js_name = setWriterVersion)]
    pub fn set_writer_version(self, value: WriterVersion) -> Self {
        let parquet_writer_version = match value {
            WriterVersion::PARQUET_1_0 => properties::WriterVersion::PARQUET_1_0,
            WriterVersion::PARQUET_2_0 => properties::WriterVersion::PARQUET_2_0,
        };
        Self {
            0: self.0.set_writer_version(parquet_writer_version),
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
}

impl WriterPropertiesBuilder {
    pub fn new_from_rust() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder {
            0: properties::WriterProperties::builder(),
        }
    }
}

// #[wasm_bindgen]
// pub struct WriterProperties(parquet::file::properties::WriterProperties);

// impl WriterProperties {
//     /// Returns builder for writer properties with default values.
//     pub fn builder() -> WriterPropertiesBuilder {
//         WriterPropertiesBuilder::with_defaults()
//     }

//     /// Returns data page size limit.
//     #[wasm_bindgen(getter, js_name = dataPagesizeLimit)]
//     pub fn data_pagesize_limit(&self) -> usize {
//         self.data_pagesize_limit
//     }

//     /// Returns dictionary page size limit.
//     pub fn dictionary_pagesize_limit(&self) -> usize {
//         self.dictionary_pagesize_limit
//     }

//     /// Returns configured batch size for writes.
//     ///
//     /// When writing a batch of data, this setting allows to split it internally into
//     /// smaller batches so we can better estimate the size of a page currently being
//     /// written.
//     pub fn write_batch_size(&self) -> usize {
//         self.write_batch_size
//     }

//     /// Returns maximum number of rows in a row group.
//     pub fn max_row_group_size(&self) -> usize {
//         self.max_row_group_size
//     }

//     /// Returns configured writer version.
//     pub fn writer_version(&self) -> WriterVersion {
//         self.writer_version
//     }

//     /// Returns `created_by` string.
//     pub fn created_by(&self) -> &str {
//         &self.created_by
//     }

//     /// Returns `key_value_metadata` KeyValue pairs.
//     pub fn key_value_metadata(&self) -> &Option<Vec<KeyValue>> {
//         &self.key_value_metadata
//     }

//     /// Returns encoding for a data page, when dictionary encoding is enabled.
//     /// This is not configurable.
//     #[inline]
//     pub fn dictionary_data_page_encoding(&self) -> Encoding {
//         // PLAIN_DICTIONARY encoding is deprecated in writer version 1.
//         // Dictionary values are encoded using RLE_DICTIONARY encoding.
//         Encoding::RLE_DICTIONARY
//     }

//     /// Returns encoding for dictionary page, when dictionary encoding is enabled.
//     /// This is not configurable.
//     #[inline]
//     pub fn dictionary_page_encoding(&self) -> Encoding {
//         // PLAIN_DICTIONARY is deprecated in writer version 1.
//         // Dictionary is encoded using plain encoding.
//         Encoding::PLAIN
//     }

//     /// Returns encoding for a column, if set.
//     /// In case when dictionary is enabled, returns fallback encoding.
//     ///
//     /// If encoding is not set, then column writer will choose the best encoding
//     /// based on the column type.
//     pub fn encoding(&self, col: &ColumnPath) -> Option<Encoding> {
//         self.column_properties
//             .get(col)
//             .and_then(|c| c.encoding())
//             .or_else(|| self.default_column_properties.encoding())
//     }

//     /// Returns compression codec for a column.
//     pub fn compression(&self, col: &ColumnPath) -> Compression {
//         self.column_properties
//             .get(col)
//             .and_then(|c| c.compression())
//             .or_else(|| self.default_column_properties.compression())
//             .unwrap_or(DEFAULT_COMPRESSION)
//     }

//     /// Returns `true` if dictionary encoding is enabled for a column.
//     pub fn dictionary_enabled(&self, col: &ColumnPath) -> bool {
//         self.column_properties
//             .get(col)
//             .and_then(|c| c.dictionary_enabled())
//             .or_else(|| self.default_column_properties.dictionary_enabled())
//             .unwrap_or(DEFAULT_DICTIONARY_ENABLED)
//     }

//     /// Returns `true` if statistics are enabled for a column.
//     pub fn statistics_enabled(&self, col: &ColumnPath) -> bool {
//         self.column_properties
//             .get(col)
//             .and_then(|c| c.statistics_enabled())
//             .or_else(|| self.default_column_properties.statistics_enabled())
//             .unwrap_or(DEFAULT_STATISTICS_ENABLED)
//     }

//     /// Returns max size for statistics.
//     /// Only applicable if statistics are enabled.
//     pub fn max_statistics_size(&self, col: &ColumnPath) -> usize {
//         self.column_properties
//             .get(col)
//             .and_then(|c| c.max_statistics_size())
//             .or_else(|| self.default_column_properties.max_statistics_size())
//             .unwrap_or(DEFAULT_MAX_STATISTICS_SIZE)
//     }
// }
