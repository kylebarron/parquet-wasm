use crate::arrow2::ranged_reader::{RangeOutput, RangedAsyncReader};
use crate::arrow2::reader_async::{create_reader, read_parquet_metadata_async};
use crate::common::fetch::{get_content_length, make_range_request};
use crate::log;
use arrow2::datatypes::Schema;
use arrow2::error::Error as ArrowError;
use arrow2::error::Result as ArrowResult;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::FileMetaData;
use arrow2::io::parquet::read::{
    infer_schema, read_columns_async, read_columns_many_async, read_metadata_async,
    RowGroupDeserializer,
};
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use crate::utils::copy_vec_to_uint8_array;
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use js_sys::Uint8Array;
use std::io::Cursor;

/// Asynchronous implementation of ParquetFile
#[wasm_bindgen]
pub struct AsyncParquetFile {
    url: String,
    content_length: usize,
    metadata: FileMetaData,
    reader: RangedAsyncReader,
    schema: Schema,
}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> Result<AsyncParquetFile, JsValue> {
        let content_length = get_content_length(url.clone()).await?;

        let mut reader = create_reader(url.clone(), content_length);
        let metadata = read_metadata_async(&mut reader).await.unwrap();
        // let metadata = read_parquet_metadata_async(url.clone(), content_length).await?;
        let schema = infer_schema(&metadata).unwrap();

        Ok(Self {
            url,
            content_length,
            metadata,
            reader,
            schema,
        })
    }

    #[wasm_bindgen]
    pub fn url(&self) -> String {
        self.url.clone()
    }

    #[wasm_bindgen]
    pub fn content_length(&self) -> usize {
        self.content_length
    }

    #[wasm_bindgen]
    pub fn num_rows(&self) -> usize {
        self.metadata.num_rows
    }

    #[wasm_bindgen]
    pub fn column_name(&self, field: usize) -> String {
        self.schema.fields[field].name.clone()
    }

    #[wasm_bindgen]
    pub fn num_row_groups(&self) -> usize {
        self.metadata.row_groups.len()
    }

    pub async fn read_row_group(self, i: usize) -> Result<Uint8Array, JsValue> {
        // let (mpsc_sender, mpsc_receiver) = mpsc::channel::<Vec<u8>>(100);
        let url = self.url.clone();

        let range_get = Box::new(move |start: u64, length: usize| {
            let url = url.clone();
            // let local_mpsc_sender = mpsc_sender.clone();

            let future_fn = Box::pin(async move {
                let (local_oneshot_sender, local_oneshot_receiver) = oneshot::channel::<Vec<u8>>();
                spawn_local(async move {
                    log!("Making range request");
                    let inner_data = make_range_request(url, start, length).await.unwrap();
                    // local_mpsc_sender.send(inner_data);
                    local_oneshot_sender.send(inner_data);
                });
                let data = local_oneshot_receiver.await.unwrap();

                Ok(RangeOutput { start, data })
            }) as BoxFuture<'static, std::io::Result<RangeOutput>>;

            future_fn
        });

        let min_request_size = 4 * 1024;

        // let reader_factory1 = Box::new(|| {
        //     Box::pin(futures::future::ready({})

        //         async move {
        //         let (sender2, receiver2) = oneshot::channel::<Vec<u8>>();
        //         spawn_local(async move {
        //             log!("Making range request");
        //             let inner_data = make_range_request(url, start, length).await.unwrap();
        //             sender2.send(inner_data).unwrap();
        //         });
        //         let data = receiver2.await.unwrap();

        //         Ok(RangeOutput { start, data })
        //     }) as BoxFuture<'static, std::io::Result<RangeOutput>>
        // });

        let reader_factory = || {
            Box::pin(futures::future::ready(Ok(RangedAsyncReader::new(
                self.content_length,
                min_request_size,
                range_get.clone(),
            ))))
                as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
        };

        // let's read the first row group only. Iterate over them to your liking
        let group = &self.metadata.row_groups[0];

        // no chunk size in deserializing
        let chunk_size = None;

        let fields = self.schema.fields.clone();

        // this is IO-bounded (and issues a join, thus the reader_factory)
        let column_chunks = read_columns_many_async(reader_factory, group, fields, chunk_size)
            .await
            .unwrap();

        // Create IPC writer
        let mut output_file = Vec::new();
        let options = IPCWriteOptions { compression: None };
        let mut writer = IPCStreamWriter::new(&mut output_file, options);
        writer.start(&self.schema, None).unwrap();

        // this is CPU-bounded and should be sent to a separate thread-pool.
        // We do it here for simplicity
        let chunks = RowGroupDeserializer::new(column_chunks, group.num_rows() as usize, None);
        let chunks = chunks.collect::<ArrowResult<Vec<_>>>().unwrap();
        for chunk in chunks {
            // let chunk2 = chunk;
            writer.write(&chunk, None);
        }

        writer.finish().unwrap();
        let array = copy_vec_to_uint8_array(output_file).unwrap();
        Ok(array)
    }
}
