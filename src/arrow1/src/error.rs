use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use thiserror::Error;
use wasm_bindgen::JsError;

#[derive(Error, Debug)]
pub enum ParquetWasmError {
    #[error(transparent)]
    ArrowError(Box<ArrowError>),

    #[error(transparent)]
    ParquetError(Box<ParquetError>),
}

pub type Result<T> = std::result::Result<T, ParquetWasmError>;
pub type WasmResult<T> = std::result::Result<T, JsError>;

impl From<ArrowError> for ParquetWasmError {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(Box::new(err))
    }
}

impl From<ParquetError> for ParquetWasmError {
    fn from(err: ParquetError) -> Self {
        Self::ParquetError(Box::new(err))
    }
}
