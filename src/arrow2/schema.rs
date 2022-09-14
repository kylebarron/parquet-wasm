use wasm_bindgen::prelude::*;

/// Arrow Schema representing a Parquet file.
#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct ArrowSchema(arrow2::datatypes::Schema);

#[wasm_bindgen]
impl ArrowSchema {
    /// Clone this struct in wasm memory.
    #[wasm_bindgen]
    pub fn copy(&self) -> Self {
        ArrowSchema(self.0.clone())
    }
}

impl From<arrow2::datatypes::Schema> for ArrowSchema {
    fn from(schema: arrow2::datatypes::Schema) -> Self {
        ArrowSchema(schema)
    }
}

impl From<ArrowSchema> for arrow2::datatypes::Schema {
    fn from(meta: ArrowSchema) -> arrow2::datatypes::Schema {
        meta.0
    }
}
