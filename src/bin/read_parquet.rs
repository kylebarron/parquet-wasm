use clap::Parser;
use parquet_wasm::arrow1::reader::read_parquet;
use std::fs;
use std::path::PathBuf;
use std::process;

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
    let args = Args::parse();

    // Read file to buffer
    let data = fs::read(&args.input_file).expect("Unable to read file");
    let slice = data.as_slice();

    // Call read_parquet
    let arrow_ipc = read_parquet(slice)
        .map_err(|err| {
            eprintln!("Could not read parquet file: {}", err);
            process::exit(1);
        })
        .unwrap();

    // Write result to file
    fs::write(&args.output_file, arrow_ipc).expect("Unable to write file");
}
