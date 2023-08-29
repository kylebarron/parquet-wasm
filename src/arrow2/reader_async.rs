use crate::arrow2::error::ParquetWasmError;
use crate::arrow2::error::Result;
use crate::common::fetch::{create_reader, get_content_length};
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::FileMetaData;
use arrow2::io::parquet::read::RowGroupMetaData;
use arrow2::io::parquet::read::{read_columns_many_async, RowGroupDeserializer};
use arrow_wasm::arrow2::RecordBatch;
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

pub async fn _read_row_group(
    url: String,
    // content_length: Option<usize>,
    row_group_meta: &RowGroupMetaData,
    arrow_schema: &Schema,
) -> Result<RowGroupDeserializer> {
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

    let deserializer = RowGroupDeserializer::new(column_chunks, row_group_meta.num_rows(), None);
    Ok(deserializer)
}

pub async fn read_row_group(
    url: String,
    // content_length: Option<usize>,
    row_group_meta: &RowGroupMetaData,
    arrow_schema: &Schema,
) -> Result<RecordBatch> {
    let deserializer = _read_row_group(url, row_group_meta, arrow_schema).await?;

    let chunk = {
        let mut chunks = Vec::with_capacity(1);

        for maybe_chunk in deserializer {
            chunks.push(maybe_chunk?);
        }

        // Should be 1 because only reading one row group
        assert_eq!(chunks.len(), 1);
        chunks.pop().unwrap()
    };

    Ok(RecordBatch::new(arrow_schema.clone(), chunk))
}

pub async fn read_record_batch_stream(
    url: String,
) -> Result<impl futures::Stream<Item = RecordBatch>> {
    use async_stream::stream;
    let inner_stream = stream! {
        let metadata = read_metadata_async(url.clone(), None).await.unwrap();
        let compat_meta = crate::arrow2::metadata::FileMetaData::from(metadata.clone());

        let arrow_schema = compat_meta.arrow_schema().unwrap_or_else(|_| {
            let bar: Vec<arrow2::datatypes::Field> = vec![];
            arrow2::datatypes::Schema::from(bar).into()
        });
        for row_group_meta in metadata.row_groups {
            let schema = arrow_schema.clone().into();
            let deserializer = _read_row_group(url.clone(), &row_group_meta, &schema).await.unwrap();
            for maybe_chunk in deserializer {
                let chunk = maybe_chunk.unwrap();
                yield RecordBatch::new(arrow_schema.clone().into(), chunk);
            }
        }
    };
    Ok(inner_stream)
}
