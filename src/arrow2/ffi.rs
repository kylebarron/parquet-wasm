use crate::arrow2::error::ParquetWasmError;
use crate::arrow2::error::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::ffi;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::ArrowArray>);

impl From<Box<dyn Array>> for FFIArrowArray {
    fn from(array: Box<dyn Array>) -> Self {
        Self(Box::new(ffi::export_array_to_c(array)))
    }
}

impl FFIArrowArray {
    fn import(self, data_type: DataType) -> Result<Box<dyn Array>> {
        let imported = unsafe { ffi::import_array_from_c(*self.0, data_type) };
        Ok(imported?)
    }
}

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::ArrowArray {
        self.0.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn free(self) {
        drop(self.0)
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0)
    }
}

#[wasm_bindgen]
pub struct FFIArrowField(Box<ffi::ArrowSchema>);

impl From<&Field> for FFIArrowField {
    fn from(field: &Field) -> Self {
        Self(Box::new(ffi::export_field_to_c(field)))
    }
}

impl TryFrom<&FFIArrowField> for arrow2::datatypes::Field {
    type Error = ParquetWasmError;

    fn try_from(field: &FFIArrowField) -> Result<Self> {
        let imported = unsafe { ffi::import_field_from_c(&field.0) };
        Ok(imported?)
    }
}

#[wasm_bindgen]
impl FFIArrowField {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::ArrowSchema {
        self.0.as_ref() as *const _
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

impl FFIArrowChunk {
    // NOTE: this todo may no longer apply:

    // TODO: The idea was to try and import data via FFI as well to make sure that the exported
    // data can _in principle_ be read back if you figure out the C ABI correctly
    // One issue here is that since the FFI structs don't support `copy`, you have to _consume_
    // this and similar structs when you import. E.g. take `self` instead of `&self`.
    // Then the next issue is in returning an object whose size is not known at compile time.
    // If we consume the struct, we need to import _all_ the data via FFI. But then we need to
    // return a Chunk or Vec of arrays... but those aren't all known at compile time. Do we need to
    // box it?
    //
    // It would probably be good to see how Polars solves this problem.
    pub fn import(self, data_types: &[&DataType]) -> Result<Chunk<Box<dyn Array>>> {
        let mut arrays: Vec<Box<dyn Array>> = vec![];
        for (i, ffi_array) in self.0.into_iter().enumerate() {
            arrays.push(ffi_array.import(data_types[i].clone())?);
        }

        Ok(Chunk::new(arrays))
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

impl FFIArrowSchema {
    pub fn import(&self, i: usize) -> Result<Field> {
        let ffi_arrow_field = &self.0[i];
        ffi_arrow_field.try_into()
    }
}

impl TryFrom<&FFIArrowSchema> for Schema {
    type Error = ParquetWasmError;

    fn try_from(schema: &FFIArrowSchema) -> Result<Self> {
        let mut fields: Vec<Field> = vec![];
        for i in 0..schema.length() {
            fields.push(schema.import(i)?);
        }

        Ok(fields.into())
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
    schema: Box<FFIArrowSchema>,
    chunks: Vec<FFIArrowChunk>,
}

impl From<(FFIArrowSchema, Vec<FFIArrowChunk>)> for FFIArrowTable {
    fn from((schema, chunks): (FFIArrowSchema, Vec<FFIArrowChunk>)) -> Self {
        Self {
            schema: Box::new(schema),
            chunks,
        }
    }
}

#[wasm_bindgen]
impl FFIArrowTable {
    #[wasm_bindgen]
    pub fn schema_length(&self) -> usize {
        self.schema.length()
    }

    #[wasm_bindgen]
    pub fn schema_addr(&self, i: usize) -> *const ffi::ArrowSchema {
        self.schema.addr(i)
    }

    #[wasm_bindgen]
    pub fn chunks_length(&self) -> usize {
        self.chunks.len()
    }

    #[wasm_bindgen]
    pub fn chunk_length(&self, i: usize) -> usize {
        self.chunks[i].length()
    }

    #[wasm_bindgen]
    pub fn array(&self, chunk: usize, column: usize) -> *const ffi::ArrowArray {
        self.chunks[chunk].addr(column)
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.schema);
        drop(self.chunks);
    }
}

impl FFIArrowTable {
    pub fn import(self) -> Result<(Schema, Vec<Chunk<Box<dyn Array>>>)> {
        let schema: Schema = self.schema.as_ref().try_into()?;
        let data_types: Vec<&DataType> = schema
            .fields
            .iter()
            .map(|field| field.data_type())
            .collect();

        let mut chunks: Vec<Chunk<Box<dyn Array>>> = vec![];
        for chunk in self.chunks.into_iter() {
            let imported = chunk.import(&data_types)?;
            chunks.push(imported);
        }

        Ok((schema, chunks))
    }
}
