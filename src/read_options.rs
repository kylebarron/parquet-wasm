use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::schema::types::SchemaDescriptor;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use crate::error::{ParquetWasmError, Result};

#[wasm_bindgen(typescript_custom_section)]
const TS_ReaderOptions: &'static str = r#"
export type ReaderOptions = {
    /* The number of rows in each batch. If not provided, the upstream parquet default is 1024. */
    batchSize?: number;
    /* Only read data from the provided row group indexes. */
    rowGroups?: number[];
    /* Provide a limit to the number of rows to be read. */
    limit?: number;
    /* Provide an offset to skip over the given number of rows. */
    offset?: number;
    /* The column names from the file to read. */
    columns?: string[];
    /* The number of concurrent requests to make in the async reader. */
    concurrency?: number;
};
"#;

#[wasm_bindgen]
extern "C" {
    /// Reader options
    #[wasm_bindgen(typescript_type = "ReaderOptions")]
    pub type ReaderOptions;
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JsReaderOptions {
    /// The number of rows in each batch. If not provided, the upstream parquet default is 1024.
    pub batch_size: Option<usize>,

    /// Only read data from the provided row group indexes
    pub row_groups: Option<Vec<usize>>,

    /// Provide a limit to the number of rows to be read
    pub limit: Option<usize>,

    /// Provide an offset to skip over the given number of rows
    pub offset: Option<usize>,

    /// The column names from the file to read.
    pub columns: Option<Vec<String>>,

    /// The number of concurrent requests to make in the async reader.
    pub concurrency: Option<usize>,
}

impl JsReaderOptions {
    pub fn apply_to_builder<T>(
        &self,
        mut builder: ArrowReaderBuilder<T>,
    ) -> Result<ArrowReaderBuilder<T>> {
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }

        if let Some(columns) = &self.columns {
            let parquet_schema = builder.parquet_schema();
            let projection_mask = generate_projection_mask(columns, parquet_schema)?;

            builder = builder.with_projection(projection_mask);
        }

        if let Some(row_groups) = &self.row_groups {
            builder = builder.with_row_groups(row_groups.clone());
        }

        Ok(builder)
    }
}

impl TryFrom<ReaderOptions> for JsReaderOptions {
    type Error = serde_wasm_bindgen::Error;

    fn try_from(value: ReaderOptions) -> std::result::Result<Self, Self::Error> {
        serde_wasm_bindgen::from_value(value.obj)
    }
}

fn generate_projection_mask<S: AsRef<str>>(
    columns: &[S],
    pq_schema: &SchemaDescriptor,
) -> Result<ProjectionMask> {
    let col_paths = pq_schema
        .columns()
        .iter()
        .map(|col| col.path().string())
        .collect::<Vec<_>>();
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| {
            let col = col.as_ref();
            let field_indices: Vec<usize> = col_paths
                .iter()
                .enumerate()
                .filter(|(_idx, path)| {
                    // identical OR the path starts with the column AND the substring is immediately followed by the
                    // path separator
                    path.as_str() == col
                        || path.starts_with(col) && {
                            let left_index = path.find(col).unwrap();
                            path.chars().nth(left_index + col.len()).unwrap() == '.'
                        }
                })
                .map(|(idx, _)| idx)
                .collect();
            if field_indices.is_empty() {
                Err(ParquetWasmError::UnknownColumn(col.to_string()))
            } else {
                Ok(field_indices)
            }
        })
        .collect::<Result<Vec<Vec<usize>>>>()?
        .into_iter()
        .flatten()
        .collect();
    let projection_mask = ProjectionMask::leaves(pq_schema, indices);
    Ok(projection_mask)
}
