

use arrow::array::{Array, StructArray};
use arrow::datatypes::{Field, Schema};
use arrow::ffi::{self, from_ffi, to_ffi};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

use crate::arrow1::error::Result;

/// Wrapper around an ArrowArray FFI struct in Wasm memory.
#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::FFI_ArrowArray>);

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowArray {
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
pub struct FFIArrowField(Box<ffi::FFI_ArrowSchema>);

#[wasm_bindgen]
impl FFIArrowField {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.0.as_ref() as *const _
    }
}

impl From<&Field> for FFIArrowField {
    fn from(_value: &Field) -> Self {
        todo!()
    }
}

/// Wrapper around a collection of FFI ArrowSchema structs in Wasm memory
#[wasm_bindgen]
pub struct FFIArrowSchema(Vec<FFIArrowField>);

#[wasm_bindgen]
impl FFIArrowSchema {
    /// The number of fields in this schema
    #[wasm_bindgen]
    pub fn length(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen]
    pub fn addr(&self, i: usize) -> *const ffi::FFI_ArrowSchema {
        self.0.get(i).unwrap().addr()
    }
}

impl From<&Schema> for FFIArrowSchema {
    fn from(value: &Schema) -> Self {
        for _field in value.fields.into_iter() {}
        todo!()
    }
}

/// Wrapper to represent an Arrow Chunk in Wasm memory, e.g. a collection of FFI ArrowArray
/// structs
#[wasm_bindgen]
pub struct FFIArrowRecordBatch {
    array: Box<ffi::FFI_ArrowArray>,
    field: Box<ffi::FFI_ArrowSchema>,
}

#[wasm_bindgen]
impl FFIArrowRecordBatch {
    /// Get the pointer to one ArrowSchema FFI struct
    /// @param i number the index of the field in the schema to use
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn field_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.field.as_ref() as *const _
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param column number The column index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self) -> *const ffi::FFI_ArrowArray {
        self.array.as_ref() as *const _
    }
}

impl From<RecordBatch> for FFIArrowRecordBatch {
    fn from(value: RecordBatch) -> Self {
        let intermediate = StructArray::from(value).into_data();
        let (out_array, out_schema) = to_ffi(&intermediate).unwrap();
        Self {
            array: Box::new(out_array),
            field: Box::new(out_schema),
        }
    }
}

impl From<FFIArrowRecordBatch> for RecordBatch {
    fn from(value: FFIArrowRecordBatch) -> Self {
        let array_data = from_ffi(*value.array, &value.field).unwrap();
        let intermediate = StructArray::from(array_data);
        
        RecordBatch::from(intermediate)
    }
}

/// Wrapper around an Arrow Table in Wasm memory (a list of FFI ArrowSchema structs plus a list of
/// lists of ArrowArray FFI structs.)
#[wasm_bindgen]
pub struct FFIArrowTable(Vec<FFIArrowRecordBatch>);

impl From<Vec<RecordBatch>> for FFIArrowTable {
    fn from(value: Vec<RecordBatch>) -> Self {
        let mut batches = Vec::with_capacity(value.len());
        for batch in value {
            batches.push(batch.into());
        }
        Self(batches)
    }
}

#[wasm_bindgen]
impl FFIArrowTable {
    #[wasm_bindgen(js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.0.len()
    }

    /// Get the pointer to one ArrowSchema FFI struct
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.0[0].field_addr()
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param chunk number The chunk index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, chunk: usize) -> *const ffi::FFI_ArrowArray {
        self.0[chunk].array_addr()
    }
}

impl From<Vec<FFIArrowRecordBatch>> for FFIArrowTable {
    fn from(batches: Vec<FFIArrowRecordBatch>) -> Self {
        Self(batches)
    }
}

impl FFIArrowTable {
    pub fn from_iterator(value: impl IntoIterator<Item = RecordBatch>) -> Self {
        let mut batches = vec![];
        for batch in value.into_iter() {
            batches.push(batch.into());
        }
        Self(batches)
    }

    pub fn try_from_iterator(
        value: impl IntoIterator<Item = arrow::error::Result<RecordBatch>>,
    ) -> Result<Self> {
        let mut batches = vec![];
        for batch in value.into_iter() {
            batches.push(batch?.into());
        }
        Ok(Self(batches))
    }
}
