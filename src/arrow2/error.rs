use arrow2::error::Error as ArrowError;
use parquet2::error::Error as ParquetError;
use thiserror::Error;
use wasm_bindgen::JsError;

#[derive(Error, Debug)]
pub enum ParquetWasmError {
    #[error(transparent)]
    ArrowError(Box<ArrowError>),

    #[error(transparent)]
    ParquetError(Box<ParquetError>),

    #[error("Internal error: `{0}`")]
    InternalError(String),

    #[cfg(feature = "async")]
    #[error("HTTP error: `{0}`")]
    HTTPError(Box<reqwest::Error>),
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

#[cfg(feature = "async")]
impl From<reqwest::Error> for ParquetWasmError {
    fn from(err: reqwest::Error) -> Self {
        Self::HTTPError(Box::new(err))
    }
}
