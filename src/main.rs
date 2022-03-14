use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use std::{fs::File, path::Path};

fn main() {
    let path = Path::new("./tests/data/1-partition-lz4.parquet");
    let file = File::open(&path).unwrap();

    // Create Parquet reader
    let file_reader = ParquetFileReader::try_new(file, None, None, None, None).unwrap();
    let schema = file_reader.schema().clone();

    // Create IPC writer
    let mut output_file = File::create("out-file-ipc.arrow").unwrap();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None).unwrap();

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = maybe_chunk.unwrap();
        writer.write(&chunk, None).unwrap();
    }

    writer.finish().unwrap();
}
