use wasm_bindgen::prelude::*;

// I can't just re-export the ParquetCompression enum because enums with #[wasm_bindgen] may only
// have number literal values
#[wasm_bindgen]
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    BROTLI,
    LZ4,
    ZSTD,
}

#[wasm_bindgen]
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Encoding {
    /// Default encoding.
    /// BOOLEAN - 1 bit per value. 0 is false; 1 is true.
    /// INT32 - 4 bytes per value.  Stored as little-endian.
    /// INT64 - 8 bytes per value.  Stored as little-endian.
    /// FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
    /// DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
    /// BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// FIXED_LEN_BYTE_ARRAY - Just the bytes.
    PLAIN,
    /// Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
    /// plain type.
    /// in a data page use RLE_DICTIONARY instead.
    /// in a Dictionary page use PLAIN instead
    PLAIN_DICTIONARY,
    /// Group packed run length encoding. Usable for definition/repetition levels
    /// encoding and Booleans (on one bit: 0 is false; 1 is true.)
    RLE,
    /// Bit packed encoding.  This can only be used if the data has a known max
    /// width.  Usable for definition/repetition levels encoding.
    BIT_PACKED,
    /// Delta encoding for integers. This can be used for int columns and works best
    /// on sorted data
    DELTA_BINARY_PACKED,
    /// Encoding for byte arrays to separate the length values and the data. The lengths
    /// are encoded using DELTA_BINARY_PACKED
    DELTA_LENGTH_BYTE_ARRAY,
    /// Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
    /// Suffixes are stored as delta length byte arrays.
    DELTA_BYTE_ARRAY,
    /// Dictionary encoding: the ids are encoded using the RLE encoding
    RLE_DICTIONARY,
    /// Encoding for floating-point data.
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of an FP value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards.
    BYTE_STREAM_SPLIT,
}
