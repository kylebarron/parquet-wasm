use crate::arrow2::error::ParquetWasmError;
use crate::arrow2::error::Result;
use crate::common::fetch::{create_reader, get_content_length};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::FileMetaData;
use arrow2::io::parquet::read::RowGroupMetaData;
use arrow2::io::parquet::read::{read_columns_many_async, RowGroupDeserializer};
use futures::future::BoxFuture;
use parquet2::read::read_metadata_async as _read_metadata_async;
use range_reader::RangedAsyncReader;

pub async fn read_metadata_async(
    url: String,
    content_length: Option<usize>,
) -> Result<FileMetaData> {
    let content_length = match content_length {
        Some(_content_length) => _content_length,
        None => get_content_length(url.clone()).await?,
    };

    let mut reader = create_reader(url, content_length, None);
    let metadata = _read_metadata_async(&mut reader).await?;
    Ok(metadata)
}

/// Check if all elements in an array are equal
/// https://sts10.github.io/2019/06/06/is-all-equal-function.html
fn all_elements_equal(arr: &[&Option<String>]) -> bool {
    if arr.is_empty() {
        return true;
    }
    let first = arr[0];
    arr.iter().all(|&item| item == first)
}

pub async fn read_row_group(
    url: String,
    // content_length: Option<usize>,
    row_group_meta: &RowGroupMetaData,
    arrow_schema: &Schema,
    chunk_fn: impl Fn(Chunk<Box<dyn Array>>) -> Chunk<Box<dyn Array>>,
) -> Result<Vec<u8>> {
    // Extract the file paths from each underlying column
    let file_paths: Vec<&Option<String>> = row_group_meta
        .columns()
        .iter()
        .map(|column_chunk| column_chunk.file_path())
        .collect();

    if !all_elements_equal(&file_paths) {
        return Err(ParquetWasmError::InternalError(
            "Row groups with unequal paths are not supported".to_string(),
        ));
    }

    // If a file path exists, append it to url
    let file_path = file_paths[0];
    let url = if let Some(file_path) = file_path {
        let mut trimmed = url.trim_end_matches('/').to_string();
        trimmed.push('/');
        trimmed.push_str(file_path);
        trimmed
    } else {
        url
    };

    // Note: for simplicity requesting the content length with a HEAD request always.
    let content_length = get_content_length(url.clone()).await.unwrap();

    let reader_factory = || {
        Box::pin(futures::future::ready(Ok(create_reader(
            url.clone(),
            content_length,
            None,
        )))) as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
    };

    // no chunk size in deserializing
    let chunk_size = None;
    let fields = arrow_schema.fields.clone();

    // this is IO-bounded (and issues a join, thus the reader_factory)
    let column_chunks = read_columns_many_async(
        reader_factory,
        row_group_meta,
        fields,
        chunk_size,
        None,
        None,
    )
    .await?;

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(arrow_schema, None)?;

    let deserializer = RowGroupDeserializer::new(column_chunks, row_group_meta.num_rows(), None);
    for maybe_chunk in deserializer {
        let chunk = chunk_fn(maybe_chunk?);
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}
