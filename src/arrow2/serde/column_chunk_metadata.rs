use parquet2::metadata::{ColumnChunkMetaData, ColumnDescriptor};
use serde::{Serialize, Deserialize};
use parquet_format_async_temp::{ColumnChunk, ColumnMetaData, Encoding};


#[derive(Serialize, Deserialize)]
#[serde(remote = "ColumnChunkMetaData")]
struct ColumnChunkMetaDataDef {
    #[serde(getter = "ColumnChunkMetaData::column_chunk")]
    column_chunk: ColumnChunk,

    #[serde(getter = "ColumnChunkMetaData::descriptor")]
    column_descr: ColumnDescriptor,
}

// Provide a conversion to construct the remote type.
impl From<ColumnChunkMetaDataDef> for ColumnChunkMetaData {
    fn from(def: ColumnChunkMetaDataDef) -> ColumnChunkMetaData {
        ColumnChunkMetaData::new(def.column_chunk, def.column_descr)
    }
}
