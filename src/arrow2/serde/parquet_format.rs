use parquet_format_async_temp::{
    ColumnChunk, ColumnCryptoMetaData, ColumnIndex, ColumnMetaData, CompressionCodec, Encoding,
    EncryptionWithColumnKey, FileMetaData, OffsetIndex, PageLocation, RowGroup, Statistics,
};
use serde::{Deserialize, Serialize, Deserializer};
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize)]
#[serde(remote = "Statistics")]
pub struct StatisticsDef {
    pub max: Option<Vec<u8>>,
    pub min: Option<Vec<u8>>,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<Vec<u8>>,
    pub min_value: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct Test {
    pub a: String,

    #[serde(with = "StatisticsDef")]
    pub stats: Statistics,
}

#[wasm_bindgen]
pub fn round_trip_statistics(input: JsValue) -> Result<JsValue, JsValue> {
    let rust_value: Test = serde_wasm_bindgen::from_value(input).unwrap();
    Ok(serde_wasm_bindgen::to_value(&rust_value).unwrap())
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ColumnChunk")]
pub struct ColumnChunkDef {
    file_path: Option<String>,
    file_offset: i64,

    #[serde(with = "ColumnMetaDataDef")]
    meta_data: Option<ColumnMetaData>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,

    #[serde(with = "ColumnCryptoMetaDataDef")]
    crypto_metadata: Option<ColumnCryptoMetaData>,
    encrypted_column_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ColumnMetaData")]
pub struct ColumnMetaDataDef {
    pub type_: Type,
    pub encodings: Vec<Encoding>,
    pub path_in_schema: Vec<String>,

    #[serde(with = "CompressionCodecDef")]
    pub codec: CompressionCodec,
    pub num_values: i64,
    pub total_uncompressed_size: i64,
    pub total_compressed_size: i64,
    pub key_value_metadata: Option<Vec<KeyValue>>,
    pub data_page_offset: i64,
    pub index_page_offset: Option<i64>,
    pub dictionary_page_offset: Option<i64>,
    pub statistics: Option<Statistics>,
    pub encoding_stats: Option<Vec<PageEncodingStats>>,
    pub bloom_filter_offset: Option<i64>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "CompressionCodec")]
pub struct CompressionCodecDef {}

#[derive(Serialize, Deserialize)]
#[serde(remote = "EncryptionWithColumnKey")]
pub struct EncryptionWithColumnKeyDef {
    /// Column path in schema *
    pub path_in_schema: Vec<String>,
    /// Retrieval metadata of column encryption key *
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "RowGroup")]
pub struct RowGroupDef {
    #[serde(with = "ColumnChunkDef")]
    columns: Vec<ColumnChunk>,
    total_byte_size: i64,
    num_rows: i64,
    sorting_columns: Option<Vec<SortingColumn>>,
    file_offset: Option<i64>,
    total_compressed_size: Option<i64>,
    ordinal: Option<i16>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "PageLocation")]
pub struct PageLocationDef {
    offset: i64,
    compressed_page_size: i32,
    first_row_index: i64,
}

#[derive(Deserialize)]
#[serde(remote = "OffsetIndex")]
pub struct OffsetIndexDef {
    #[serde(deserialize_with = "vec_page_location_def")]
    page_locations: Vec<PageLocation>,
}

fn vec_page_location_def<'de, D>(deserializer: D) -> Result<Vec<PageLocation>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper(#[serde(with = "PageLocationDef")] PageLocation);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(a)| a).collect())
}


#[derive(Serialize, Deserialize)]
#[serde(remote = "ColumnIndex")]
pub struct ColumnIndexDef {
    null_pages: Vec<bool>,
    min_values: Vec<Vec<u8>>,
    max_values: Vec<Vec<u8>>,
    boundary_order: BoundaryOrder,
    null_counts: Option<Vec<i64>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "FileMetaData")]
pub struct FileMetaDataDef {
    pub version: i32,
    pub schema: Vec<SchemaElement>,
    pub num_rows: i64,
    #[serde(with = "RowGroupDef")]
    pub row_groups: Vec<RowGroup>,
    pub key_value_metadata: Option<Vec<KeyValue>>,
    pub created_by: Option<String>,
    pub column_orders: Option<Vec<ColumnOrder>>,
    pub encryption_algorithm: Option<EncryptionAlgorithm>,
    pub footer_signing_key_metadata: Option<Vec<u8>>,
}
