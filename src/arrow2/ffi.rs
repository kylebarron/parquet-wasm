use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use arrow2::ffi;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct FFIArrowArray(ffi::ArrowArray);

impl From<Box<dyn Array>> for FFIArrowArray {
    fn from(array: Box<dyn Array>) -> Self {
        Self(ffi::export_array_to_c(array))
    }
}

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::ArrowArray {
        &self.0 as *const _
    }
}

#[wasm_bindgen]
pub struct FFIArrowField(ffi::ArrowSchema);

impl From<&Field> for FFIArrowField {
    fn from(field: &Field) -> Self {
        Self(ffi::export_field_to_c(field))
    }
}

#[wasm_bindgen]
impl FFIArrowField {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::ArrowSchema {
        &self.0 as *const _
    }
}

#[wasm_bindgen]
pub struct FFIArrowChunk(Vec<FFIArrowArray>);

impl From<Chunk<Box<dyn Array>>> for FFIArrowChunk {
    fn from(chunk: Chunk<Box<dyn Array>>) -> Self {
        // TODO: is this clone necessary here?
        let ffi_arrays: Vec<FFIArrowArray> =
            chunk.iter().map(|array| array.clone().into()).collect();
        Self(ffi_arrays)
    }
}

#[wasm_bindgen]
impl FFIArrowChunk {
    #[wasm_bindgen]
    pub fn length(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen]
    pub fn addr(&self, i: usize) -> *const ffi::ArrowArray {
        self.0.get(i).unwrap().addr()
    }
}

#[wasm_bindgen]
pub struct FFIArrowSchema(Vec<FFIArrowField>);

impl From<&Schema> for FFIArrowSchema {
    fn from(schema: &Schema) -> Self {
        let ffi_fields: Vec<FFIArrowField> =
            schema.fields.iter().map(|field| field.into()).collect();
        Self(ffi_fields)
    }
}

#[wasm_bindgen]
impl FFIArrowSchema {
    #[wasm_bindgen]
    pub fn length(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen]
    pub fn addr(&self, i: usize) -> *const ffi::ArrowSchema {
        self.0.get(i).unwrap().addr()
    }
}

#[wasm_bindgen]
pub struct FFIArrowTable {
    schema: FFIArrowSchema,
    chunks: Vec<FFIArrowChunk>,
}

impl From<(FFIArrowSchema, Vec<FFIArrowChunk>)> for FFIArrowTable {
    fn from((schema, chunks): (FFIArrowSchema, Vec<FFIArrowChunk>)) -> Self {
        Self { schema, chunks }
    }
}
