use crate::common::writer_properties::{Compression, Encoding, WriterVersion};
use wasm_bindgen::prelude::*;

impl Encoding {
    pub fn to_arrow2(self) -> arrow2::io::parquet::write::Encoding {
        match self {
            Encoding::PLAIN => arrow2::io::parquet::write::Encoding::Plain,
            Encoding::PLAIN_DICTIONARY => arrow2::io::parquet::write::Encoding::PlainDictionary,
            Encoding::RLE => arrow2::io::parquet::write::Encoding::Rle,
            Encoding::BIT_PACKED => arrow2::io::parquet::write::Encoding::BitPacked,
            Encoding::DELTA_BINARY_PACKED => {
                arrow2::io::parquet::write::Encoding::DeltaBinaryPacked
            }
            Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                arrow2::io::parquet::write::Encoding::DeltaLengthByteArray
            }
            Encoding::DELTA_BYTE_ARRAY => arrow2::io::parquet::write::Encoding::DeltaByteArray,
            Encoding::RLE_DICTIONARY => arrow2::io::parquet::write::Encoding::RleDictionary,
            Encoding::BYTE_STREAM_SPLIT => arrow2::io::parquet::write::Encoding::ByteStreamSplit,
        }
    }
}

impl Compression {
    pub fn to_arrow2(self) -> arrow2::io::parquet::write::CompressionOptions {
        match self {
            Compression::UNCOMPRESSED => {
                arrow2::io::parquet::write::CompressionOptions::Uncompressed
            }
            Compression::SNAPPY => arrow2::io::parquet::write::CompressionOptions::Snappy,
            Compression::GZIP => arrow2::io::parquet::write::CompressionOptions::Gzip,
            Compression::BROTLI => arrow2::io::parquet::write::CompressionOptions::Brotli,
            Compression::LZ4 => arrow2::io::parquet::write::CompressionOptions::Lz4,
            // Note: We pass None to use the default ZSTD compression level
            Compression::ZSTD => arrow2::io::parquet::write::CompressionOptions::Zstd(None),
            Compression::LZ4_RAW => arrow2::io::parquet::write::CompressionOptions::Lz4Raw,
        }
    }
}

impl WriterVersion {
    pub fn to_arrow2(self) -> arrow2::io::parquet::write::Version {
        match self {
            WriterVersion::V1 => arrow2::io::parquet::write::Version::V1,
            WriterVersion::V2 => arrow2::io::parquet::write::Version::V2,
        }
    }
}

/// Immutable struct to hold writing configuration for `writeParquet2`.
///
/// Use {@linkcode WriterPropertiesBuilder} to create a configuration, then call {@linkcode
/// WriterPropertiesBuilder.build} to create an instance of `WriterProperties`.
#[wasm_bindgen]
pub struct WriterProperties {
    write_options: arrow2::io::parquet::write::WriteOptions,
    encoding: arrow2::io::parquet::write::Encoding,
}

impl WriterProperties {
    pub fn get_write_options(&self) -> arrow2::io::parquet::write::WriteOptions {
        self.write_options
    }

    pub fn get_encoding(&self) -> arrow2::io::parquet::write::Encoding {
        self.encoding
    }
}

/// Builder to create a writing configuration for `writeParquet2`
///
/// Call {@linkcode build} on the finished builder to create an immputable {@linkcode WriterProperties} to pass to `writeParquet2`
#[wasm_bindgen]
pub struct WriterPropertiesBuilder {
    write_options: arrow2::io::parquet::write::WriteOptions,
    encoding: arrow2::io::parquet::write::Encoding,
}

#[wasm_bindgen]
impl WriterPropertiesBuilder {
    /// Returns default state of the builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let write_options = arrow2::io::parquet::write::WriteOptions {
            write_statistics: true,
            compression: arrow2::io::parquet::write::CompressionOptions::Snappy,
            version: arrow2::io::parquet::write::Version::V2,
        };
        let encoding = arrow2::io::parquet::write::Encoding::Plain;
        Self {
            write_options,
            encoding,
        }
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    #[wasm_bindgen]
    pub fn build(self) -> WriterProperties {
        WriterProperties {
            write_options: self.write_options,
            encoding: self.encoding,
        }
    }

    /// Sets writer version.
    #[wasm_bindgen(js_name = setWriterVersion)]
    pub fn set_writer_version(mut self, value: WriterVersion) -> Self {
        let write_options = arrow2::io::parquet::write::WriteOptions {
            write_statistics: self.write_options.write_statistics,
            compression: self.write_options.compression,
            version: value.to_arrow2(),
        };
        self.write_options = write_options;
        self
    }

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
    pub fn set_encoding(mut self, value: Encoding) -> Self {
        self.encoding = value.to_arrow2();
        self
    }

    /// Sets compression codec for any column.
    #[wasm_bindgen(js_name = setCompression)]
    pub fn set_compression(mut self, value: Compression) -> Self {
        let write_options = arrow2::io::parquet::write::WriteOptions {
            write_statistics: self.write_options.write_statistics,
            compression: value.to_arrow2(),
            version: self.write_options.version,
        };
        self.write_options = write_options;
        self
    }

    /// Sets flag to enable/disable statistics for any column.
    #[wasm_bindgen(js_name = setStatisticsEnabled)]
    pub fn set_statistics_enabled(mut self, value: bool) -> Self {
        let write_options = arrow2::io::parquet::write::WriteOptions {
            write_statistics: value,
            compression: self.write_options.compression,
            version: self.write_options.version,
        };
        self.write_options = write_options;
        self
    }
}

impl Default for WriterPropertiesBuilder {
    fn default() -> Self {
        WriterPropertiesBuilder::new()
    }
}
