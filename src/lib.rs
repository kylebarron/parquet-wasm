extern crate web_sys;

mod utils;

use arrow::array::{ArrayRef, BooleanArray, Int32Array};
use arrow::compute::filter;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result as ArrowResult;
use arrow::ipc::writer::{StreamWriter};
use arrow::record_batch::{RecordBatchReader, RecordBatch};

use js_sys::Uint8Array;

use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::util::cursor::SliceableCursor;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::error;
//use std::rc::Arc;
use std::sync::Arc;
use std::sync::Mutex;

use wasm_bindgen;
use wasm_bindgen::prelude::*;

#[macro_use]
extern crate lazy_static;


// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[cfg(target_arch = "wasm32")]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! log {
    ( $( $t:tt )* ) => {
        println!("LOG - {}", format!( $( $t )* ));
    }
}


lazy_static! {
    static ref DATA: Mutex<RiotData> = Mutex::new(new_riot_data());
}


struct RiotData {
    geo_physical_risk_type_name_2_record_batch: HashMap<String, PhysicalRiskRecordBatch>,
    rcp_id_2_record_batch: HashMap<i32, Arc<Vec<u8>>>,
}

fn new_riot_data() -> RiotData {
    RiotData {
        geo_physical_risk_type_name_2_record_batch: HashMap::new(),
        rcp_id_2_record_batch: HashMap::new(),
    }
}

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/

/*
#[wasm_bindgen]
extern {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, read-parquet-browser!");
}
*/


pub struct PhysicalRiskRecordBatch {
    external_name: String,
    record_set: RecordBatch,
}


impl PhysicalRiskRecordBatch {

}

#[wasm_bindgen]
pub fn read_geo_physical_risk_parquet(physical_risk_type_name: &str, parquet_file_bytes: &[u8])
                                      -> Result<(), JsValue> {

    log!("In rust, parquet bytes array has size: {}", parquet_file_bytes.len());

    let parquet_bytes_as_vec = parquet_file_bytes.to_vec();
    let parquet_vec_arc = Arc::new(parquet_bytes_as_vec);
    let sliceable_cursor = SliceableCursor::new(parquet_vec_arc);
    log!("created sliceable cursor");

    let parquet_reader_result
        = SerializedFileReader::new(sliceable_cursor);

    match parquet_reader_result {
        Ok(parquet_reader) => {
            log!("created parquet reader mk2");
            let pq_file_metadata = parquet_reader.metadata().file_metadata();
            let pq_row_count = pq_file_metadata.num_rows() as usize;
            //log!("got parquet metadata: {:?}", pq_file_metadata);


            let mut arrow_reader
                = ParquetFileArrowReader::new(Arc::new(parquet_reader));

            log!("Got an arrow reader.");

            let mut record_batch_reader_result
                = arrow_reader.get_record_reader(pq_row_count);

            log!("Got an arrow batch reader.");


            match record_batch_reader_result {
                Ok(record_batch_reader) => {
                    log!("Got an arrow record batch reader.");

                    for maybe_record_batch in record_batch_reader {
                        let record_batch = maybe_record_batch.expect("why not read batch");
                        //if record_batch.num_rows() > 0 {
                        log!("Read {} records.", &record_batch.num_rows());
                        let new_parquet_record_batch = PhysicalRiskRecordBatch {
                            external_name: "testing".to_string(),
                            record_set: record_batch,
                        };

                        let data: &mut RiotData = &mut *DATA.lock().unwrap();

                        data.geo_physical_risk_type_name_2_record_batch
                            .insert(
                                physical_risk_type_name.to_string()
                                , new_parquet_record_batch
                            );
                    }
                },
                Err(batch_err) => {
                    log!("Failed to get an arrow record batch reader.");
                    return Err(JsValue::from_str(format!("{}", batch_err).as_str()))
                }
            }

            log!("Finished reading records into arrow.");
            Ok(())

        },
        Err(parquet_reader_err) => {
            log!("Failed to create parquet reader: {}", parquet_reader_err);
            Err(JsValue::from_str(format!("{}", parquet_reader_err).as_str()))
        }
    }

}

#[wasm_bindgen]
pub fn find_for_rcp(physical_risk_type_name: &str, rcp_id: i32) -> Result<Uint8Array, JsValue> {
    let cached_result_ipc = internal_find_for_rcp(physical_risk_type_name, rcp_id)?;
    return Ok(unsafe {Uint8Array::view(&cached_result_ipc)});
}

pub fn internal_find_for_rcp(physical_risk_type_name: &str, rcp_id: i32) -> Result<Arc<Vec<u8>>, JsValue>{
    let riot_data: &mut RiotData = &mut *DATA.lock().unwrap();

    if riot_data.rcp_id_2_record_batch.contains_key(&rcp_id) {
        log!("already got rcp: {} in cache", rcp_id);
        let cached_result_ipc
            = riot_data.rcp_id_2_record_batch.get(&rcp_id).unwrap();
        return Ok(cached_result_ipc.clone());
    } else {
        let physical_risk_geo_data_option
            = riot_data.geo_physical_risk_type_name_2_record_batch.get(physical_risk_type_name);
        match physical_risk_geo_data_option {
            Some(physical_risk_geo_data) => {
                let physical_risk_data_for_rcp_result
                    = filter_for_rcp(rcp_id, &physical_risk_geo_data.record_set);
                match physical_risk_data_for_rcp_result {
                    Ok(physical_risk_data_for_rcp) => {
                        log!("Filtered RCP vals count: {}", physical_risk_data_for_rcp.num_rows());

                        let result_buf: Vec<u8> = Vec::new();
                        let mut arrow_stream_writer_result
                            = StreamWriter::try_new(
                            result_buf
                            , &physical_risk_data_for_rcp.schema()
                        );
                        match arrow_stream_writer_result {
                            Ok(mut arrow_stream_writer) => {
                                let rec_batch_write_result
                                    = arrow_stream_writer.write(&physical_risk_data_for_rcp);
                                match rec_batch_write_result {
                                    Ok(_0) => {
                                        let finish_write_result = arrow_stream_writer.finish();
                                        match finish_write_result {
                                            Ok(_0) => {
                                                let completed_result
                                                    = arrow_stream_writer.into_inner();
                                                match completed_result {
                                                    Ok(stream_data) => {
                                                        log!("Got stream writer data :)");
                                                        riot_data
                                                            .rcp_id_2_record_batch
                                                            .insert(rcp_id, Arc::new(stream_data));
                                                        let cached_result_ipc
                                                            = riot_data.rcp_id_2_record_batch.get(&rcp_id).unwrap();

                                                        return Ok(cached_result_ipc.clone());
                                                    },
                                                    Err(completed_err) => {
                                                        let err_str = format!(
                                                            "Completing write failed: {}"
                                                            ,completed_err
                                                        );
                                                        log!("{}", err_str);
                                                        return Err(JsValue::from_str(err_str.as_str()));
                                                    }
                                                }
                                            },
                                            Err(finsh_batch_write_err) => {
                                                let err_str
                                                    = format!(
                                                    "Failed to finish record batch write: {}"
                                                    , finsh_batch_write_err
                                                );
                                                log!("{}", err_str);
                                                return Err(JsValue::from_str(err_str.as_str()));
                                            }
                                        }
                                    },
                                    Err(rec_batch_write_err) => {
                                        let err_str
                                            = format!(
                                            "Failed to write rec batch into stream reader: {}"
                                            , rec_batch_write_err
                                        );
                                        log!("{}", err_str);
                                        return Err(JsValue::from_str(err_str.as_str()));
                                    }
                                }
                            },
                            Err(create_stream_writer_err) => {
                                let err_str = format!(
                                    "Failed to create arrow stream reader: {}"
                                    , create_stream_writer_err
                                );
                                log!("{}", err_str);
                                return Err(JsValue::from_str(err_str.as_str()));
                            }
                        }

                    },
                    Err(arrow_err) => {
                        let err_str
                            = format!("Failed to find rcp vals: {} for physical risk: {} due to: {}"
                                      , rcp_id, physical_risk_type_name, arrow_err);
                        log!("{}",err_str);
                        return Err(JsValue::from_str(err_str.as_str()));
                    }
                }
            },
            None => {
                let err_str
                    = format!("Failed to find record batch for physical risk type: {}"
                              , physical_risk_type_name);
                log!("{}", err_str);
                return Err(JsValue::from_str(err_str.as_str()));
            }
        }
    }

}

/*pub fn actually_find_for_rcp(physical_risk_type_name: &str, rcp_id: i32) -> Result<(), JsValue> {

}*/

pub fn filter_for_rcp(rcp_id: i32, batch: &RecordBatch) -> ArrowResult<RecordBatch> {

    let filter_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|value| Some(value == Some(rcp_id)))
        .collect::<BooleanArray>()
        ;

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for idx in 00..batch.num_columns() {
        let array = batch.column(idx).as_ref();
        let filtered = filter(array, &filter_array)?;
        arrays.push(filtered);
    }

    RecordBatch::try_new(batch.schema(), arrays)
}

#[wasm_bindgen]
pub fn init() {
    log!("init - start...");

    utils::set_panic_hook();

    log!("init - complete.");
}




#[cfg(test)]
mod tests {

    use super::*;

    //#[cfg(not(target_arch = "wasm32"))]
    use std::convert::TryInto;
    //#[cfg(not(target_arch = "wasm32"))]
    use std::fs::File;
    //#[cfg(not(target_arch = "wasm32"))]
    use std::io::Read;


    //#[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_read_parquet() {

        let filename = "./data/water-stress_rcp26and85_2020-2040-10.parquet";

        let mut f = File::open(&filename).expect("no data file found");
        let metadata = std::fs::metadata(&filename).expect("unable to read data file fs metadata");
        let file_size = metadata.len();
        let mut buffer = vec![0; metadata.len() as usize];
        f.read(&mut buffer).expect("buffer overflow reading data file");

        read_geo_physical_risk_parquet("testing",&buffer);

        println!("read_parquet finished");

        internal_find_for_rcp("testing",2);

        assert!(true);
    }


    /*fn my_vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
        v.try_into()
            .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
    }*/

}


