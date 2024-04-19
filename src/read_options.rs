use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

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
};
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "ReaderOptions")]
    pub type ReaderOptions;
}

#[derive(Serialize, Deserialize, Default)]
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
}

impl TryFrom<ReaderOptions> for JsReaderOptions {
    type Error = serde_wasm_bindgen::Error;

    fn try_from(value: ReaderOptions) -> Result<Self, Self::Error> {
        serde_wasm_bindgen::from_value(value.obj)
    }
}
