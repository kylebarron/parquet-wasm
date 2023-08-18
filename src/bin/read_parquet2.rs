use clap::Parser;
// use parquet_wasm::arrow2::reader::read_parquet;
// use std::fs;
use std::path::PathBuf;
// use std::process;

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
    // This doesn't work right now because WasmResult doesn't implement Debug
    todo!()
    // let args = Args::parse();

    // // Read file to buffer
    // let data = fs::read(&args.input_file).expect("Unable to read file");
    // let slice = data.as_slice();

    // // Call read_parquet
    // let ipc_buffer = read_parquet(slice)
    //     .unwrap().into_ipc().unwrap();

    // // Write result to file
    // fs::write(&args.output_file, ipc_buffer).expect("Unable to write file");
}
