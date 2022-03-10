use arrow::error::ArrowError;
use arrow::ipc::writer::{FileWriter, StreamWriter};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::sync::Arc;
use std::{fs::File, path::Path};

fn main() {
    let path = Path::new("./data/1-partition-snappy.parquet");
    let file = File::open(&path).unwrap();

    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = parquet_reader.metadata();
    let parquet_file_metadata = parquet_metadata.file_metadata();
    let row_count = parquet_file_metadata.num_rows() as usize;

    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
    let record_batch_reader = arrow_reader.get_record_reader(row_count).unwrap();
    let arrow_schema = arrow_reader.get_schema().unwrap();

    let mut record_batches: Vec<RecordBatch> = Vec::new();
    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        record_batches.push(record_batch);
    }

    let mut file = File::create("out-file-ipc.arrow").unwrap();
    let mut writer = FileWriter::try_new(&mut file, &arrow_schema).unwrap();
    let result: Result<Vec<()>, ArrowError> = record_batches
        .iter()
        .map(|batch| writer.write(batch))
        .collect();
    result.unwrap();
    writer.finish().unwrap();

    let mut file = File::create("out-stream-ipc.arrow").unwrap();
    let mut writer = StreamWriter::try_new(&mut file, &arrow_schema).unwrap();
    let result: Result<Vec<()>, ArrowError> = record_batches
        .iter()
        .map(|batch| writer.write(batch))
        .collect();
    result.unwrap();
    writer.finish().unwrap();
}

