use clap::Parser;
// use parquet_wasm::arrow2::writer::write_parquet;
// use parquet_wasm::arrow2::writer_properties::WriterPropertiesBuilder;
// use parquet_wasm::common::writer_properties::Compression;
// use std::fs;
use std::path::PathBuf;
// use std::process;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to input file
    #[clap(short, long)]
    input_file: PathBuf,

    /// Path to output file
    #[clap(short, long)]
    output_file: PathBuf,
}

fn main() {
    // `wasm_bindgen::JsError` doesn't implement `std::fmt::Debug`
    todo!()
    // let args = Args::parse();

    // // Read file to buffer
    // let data = fs::read(&args.input_file).expect("Unable to read file");
    // let slice = data.as_slice();

    // // Call read_parquet
    // let writer_properties = WriterPropertiesBuilder::new()
    //     .set_compression(Compression::SNAPPY)
    //     .build();

    // let table = Table::from_ipc(slice.to_vec()).unwrap();
    // let arrow_ipc = write_parquet(slice, writer_properties)
    //     .map_err(|err| {
    //         eprintln!("Could not write parquet file: {}", err);
    //         process::exit(1);
    //     })
    //     .unwrap();

    // // Write result to file
    // fs::write(&args.output_file, arrow_ipc).expect("Unable to write file");
}
