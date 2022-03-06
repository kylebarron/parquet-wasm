import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import {readParquet, writeParquet} from "parquet-wasm";

// window.wasm = wasm;
window.arrow = arrow;

// const filePath = "./water-stress_rcp26and85_2020-2040-10.parquet";

// const filePath = "./data/2-partition-brotli.parquet";
// const filePath = "./data/1-partition-gzip.parquet";
const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/1-partition-snappy.parquet";
// const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/2-partition-brotli.parquet";
// const filePath = "./data/2-partition-zstd.parquet";
// const filePath = "./data/1-partition-lz4.parquet";
// const filePath = "./data/part.parquet";
// const filePath = "./data/nz-small.parquet";
// const filePath = "./data/2021-01-01_performance_fixed_tiles.parquet";
// const filePath = './water-stress_rcp26and85_2020-2040-10.parquet'
// const filePath = './test.parquet'

// const filePath = './data/works.parquet';
// const filePath = './data/not_work.parquet';

async function fetchData() {
  let fileByteArray;
  try {
    let fetchResponse = await fetch(filePath);
    fileByteArray = new Uint8Array(await fetchResponse.arrayBuffer());
  } catch (fetchErr) {
    console.error("Fetch error: " + fetchErr);
  }

  console.log("Parquet data bytelength: " + fileByteArray.byteLength);

  const arrow_result_ipc_msg_bytes = readParquet(fileByteArray);
  console.log("finished reading");
  window.data = arrow_result_ipc_msg_bytes;

  return arrow_result_ipc_msg_bytes;
}

function saveFile(bytes, fname) {
  const blob = new Blob([bytes], {
    type: "application/pdf",
  });
  const link = document.createElement("a");
  link.href = window.URL.createObjectURL(blob);
  link.download = fname;
  link.click();
}

async function main() {
  console.time("fetchData");
  const arrow_result_ipc_msg_bytes = await fetchData();
  console.timeEnd("fetchData");

  const table = arrow.tableFromIPC(arrow_result_ipc_msg_bytes);
  window.table = table;
  console.log("table", table);

  const fileBytes = arrow.tableToIPC(table, "file");
  const test = writeParquet(fileBytes);
  window.written_parquet = test;

  // saveFile(test, "written_parquet.parquet");

  console.log("end of js");
}

console.log("trigger fetch data");
main();
