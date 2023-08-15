use crate::arrow2::error::ParquetWasmError;
use crate::arrow2::error::Result;
use arrow2::array::{Array, StructArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::ffi;
use wasm_bindgen::prelude::*;

type ArrowTable = Vec<Chunk<Box<dyn Array>>>;

/// Wrapper around an ArrowArray FFI struct in Wasm memory.
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

/// Wrapper around an ArrowSchema FFI struct in Wasm memory.
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

/// Wrapper an Arrow RecordBatch stored as FFI in Wasm memory.
#[wasm_bindgen]
pub struct FFIArrowRecordBatch {
    field: Box<ffi::ArrowSchema>,
    array: Box<ffi::ArrowArray>,
}

impl FFIArrowRecordBatch {
    pub fn new(field: Box<ffi::ArrowSchema>, array: Box<ffi::ArrowArray>) -> Self {
        Self { field, array }
    }

    pub fn from_chunk(chunk: Chunk<Box<dyn Array>>, schema: Schema) -> Self {
        let data_type = DataType::Struct(schema.fields);
        let struct_array = StructArray::try_new(data_type.clone(), chunk.to_vec(), None).unwrap();
        let field = Field::new("", data_type, false).with_metadata(schema.metadata);

        Self {
            field: Box::new(ffi::export_field_to_c(&field)),
            array: Box::new(ffi::export_array_to_c(struct_array.boxed())),
        }
    }
}

#[wasm_bindgen]
impl FFIArrowRecordBatch {
    #[wasm_bindgen]
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
        self.array.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn field_addr(&self) -> *const ffi::ArrowSchema {
        self.field.as_ref() as *const _
    }
}

/// Wrapper to represent an Arrow Chunk in Wasm memory, e.g. a  collection of FFI ArrowArray
/// structs
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

/// Wrapper around a collection of FFI ArrowSchema structs in Wasm memory
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

/// Wrapper around an Arrow Table in Wasm memory (a list of FFIArrowRecordBatch objects.)
#[wasm_bindgen]
pub struct FFIArrowTable(Vec<FFIArrowRecordBatch>);

impl FFIArrowTable {
    pub fn new(batches: Vec<FFIArrowRecordBatch>) -> Self {
        Self(batches)
    }
}

#[wasm_bindgen]
impl FFIArrowTable {
    /// Get the total number of record batches in the table
    #[wasm_bindgen(js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.0.len()
    }

    /// Get the pointer to one ArrowSchema FFI struct
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::ArrowSchema {
        // Note: this assumes that every record batch has the same schema
        self.0[0].field_addr()
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param chunk number The chunk index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, chunk: usize) -> *const ffi::ArrowArray {
        self.0[chunk].array_addr()
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0);
    }
}

impl FFIArrowTable {
    pub fn import(self) -> Result<(Schema, ArrowTable)> {
        todo!()
        // let schema: Schema = self.schema.as_ref().try_into()?;
        // let data_types: Vec<&DataType> = schema
        //     .fields
        //     .iter()
        //     .map(|field| field.data_type())
        //     .collect();

        // let mut chunks: Vec<Chunk<Box<dyn Array>>> = vec![];
        // for chunk in self.chunks.into_iter() {
        //     let imported = chunk.import(&data_types)?;
        //     chunks.push(imported);
        // }

        // Ok((schema, chunks))
    }
}
