import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm";

window.wasm = wasm;
window.arrow = arrow;
const filePath = "./water-stress_rcp26and85_2020-2040-10.parquet";

// const filePath = "./data/1-partition-brotli.parquet";
// const filePath = "./data/1-partition-gzip.parquet";
// const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/1-partition-snappy.parquet";
// const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/1-partition-zstd.parquet";
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

  const arrow_result_ipc_msg_bytes = wasm.read_parquet2(fileByteArray);
  console.log("finished reading");
  window.data = arrow_result_ipc_msg_bytes;

  // var blob=new Blob([arrow_result_ipc_msg_bytes], {type: "application/pdf"});// change resultByte to bytes
  // var link=document.createElement('a');
  // link.href=window.URL.createObjectURL(blob);
  // link.download=filePath + '.arrow';
  // link.click();

  return arrow_result_ipc_msg_bytes;
}

async function main() {
  console.time("fetchData");
  const arrow_result_ipc_msg_bytes = await fetchData();
  console.timeEnd("fetchData");

  const table = arrow.tableFromIPC(arrow_result_ipc_msg_bytes);
  window.table = table;
  console.log('table', table);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
