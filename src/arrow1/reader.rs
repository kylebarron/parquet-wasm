use crate::arrow1::error::Result;
use crate::arrow1::metadata::ParquetMetaData;
use arrow_wasm::arrow1::error::WasmResult;
use arrow_wasm::arrow1::Table;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::reader::{FileReader, SerializedFileReader};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ReadOptions {
    row_groups: Option<Vec<usize>>,
}

/// Very important: you must call `free()` once done with this
#[wasm_bindgen]
pub struct ParquetFile {
    buffer: Bytes,
    meta: ArrowReaderMetadata,
}

#[wasm_bindgen]
impl ParquetFile {
    #[wasm_bindgen(constructor)]
    pub fn new(parquet_file: Vec<u8>) -> WasmResult<ParquetFile> {
        let buffer = Bytes::from(parquet_file);
        let meta = ArrowReaderMetadata::load(&buffer, Default::default())?;
        Ok(Self { buffer, meta })
    }

    pub fn read(&self, parquet_file: Vec<u8>) -> WasmResult<Table> {
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.buffer.clone(),
            self.meta.clone(),
        );

        todo!()
    }

    pub fn read_row_group(&self, i: usize) {
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.buffer.clone(),
            self.meta.clone(),
        );
        // ProjectionMask::
        // builder.with_projection(mask)
        todo!()
        // let x = self.0.to_owned().build();
        // let x = self.0.clone().build();
    }
}

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow and parquet crates
pub fn read_parquet(parquet_file: Vec<u8>) -> Result<Table> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let num_row_groups = builder.metadata().num_row_groups();

    // builder.w
    let reader = builder.build()?;

    let mut batches = Vec::with_capacity(num_row_groups);

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(batches))
}

pub fn read_metadata(parquet_file: Vec<u8>) -> Result<ParquetMetaData> {
    let cursor = Bytes::from(parquet_file);
    let reader = SerializedFileReader::new(cursor)?;
    let metadata = reader.metadata();
    Ok(metadata.clone().into())
}

// pub fn read_schema
