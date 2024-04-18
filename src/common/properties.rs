use wasm_bindgen::prelude::*;

/// Supported compression algorithms.
///
/// Codecs added in format version X.Y can be read by readers based on X.Y and later.
/// Codec support may vary between readers based on the format version and
/// libraries available at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
#[wasm_bindgen]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    BROTLI,
    /// @deprecated as of Parquet 2.9.0.
    /// Switch to LZ4_RAW
    LZ4,
    ZSTD,
    LZ4_RAW,
    LZO,
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
            Compression::LZ4_RAW => parquet::basic::Compression::LZ4_RAW,
            Compression::LZO => parquet::basic::Compression::LZO,
        }
    }
}

impl From<parquet::basic::Compression> for Compression {
    fn from(x: parquet::basic::Compression) -> Compression {
        match x {
            parquet::basic::Compression::UNCOMPRESSED => Compression::UNCOMPRESSED,
            parquet::basic::Compression::SNAPPY => Compression::SNAPPY,
            parquet::basic::Compression::GZIP(_) => Compression::GZIP,
            parquet::basic::Compression::BROTLI(_) => Compression::BROTLI,
            parquet::basic::Compression::LZ4 => Compression::LZ4,
            parquet::basic::Compression::ZSTD(_) => Compression::ZSTD,
            parquet::basic::Compression::LZ4_RAW => Compression::LZ4_RAW,
            parquet::basic::Compression::LZO => Compression::LZO,
        }
    }
}

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

impl From<Encoding> for parquet::basic::Encoding {
    fn from(x: Encoding) -> parquet::basic::Encoding {
        match x {
            Encoding::PLAIN => parquet::basic::Encoding::PLAIN,
            Encoding::PLAIN_DICTIONARY => parquet::basic::Encoding::PLAIN_DICTIONARY,
            Encoding::RLE => parquet::basic::Encoding::RLE,
            #[allow(deprecated)]
            Encoding::BIT_PACKED => parquet::basic::Encoding::BIT_PACKED,
            Encoding::DELTA_BINARY_PACKED => parquet::basic::Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_LENGTH_BYTE_ARRAY => parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY => parquet::basic::Encoding::DELTA_BYTE_ARRAY,
            Encoding::RLE_DICTIONARY => parquet::basic::Encoding::RLE_DICTIONARY,
            Encoding::BYTE_STREAM_SPLIT => parquet::basic::Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

impl From<parquet::basic::Encoding> for Encoding {
    fn from(x: parquet::basic::Encoding) -> Encoding {
        match x {
            parquet::basic::Encoding::PLAIN => Encoding::PLAIN,
            parquet::basic::Encoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
            parquet::basic::Encoding::RLE => Encoding::RLE,
            #[allow(deprecated)]
            parquet::basic::Encoding::BIT_PACKED => Encoding::BIT_PACKED,
            parquet::basic::Encoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            parquet::basic::Encoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            parquet::basic::Encoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
            parquet::basic::Encoding::BYTE_STREAM_SPLIT => Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

/// The Parquet version to use when writing
#[allow(non_camel_case_types)]
#[wasm_bindgen]
pub enum WriterVersion {
    V1,
    V2,
}

impl From<WriterVersion> for parquet::file::properties::WriterVersion {
    fn from(x: WriterVersion) -> parquet::file::properties::WriterVersion {
        match x {
            WriterVersion::V1 => parquet::file::properties::WriterVersion::PARQUET_1_0,
            WriterVersion::V2 => parquet::file::properties::WriterVersion::PARQUET_2_0,
        }
    }
}
