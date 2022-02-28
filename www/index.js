import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm";

window.arrow = arrow;
// const filePath = "./water-stress_rcp26and85_2020-2040-10.parquet";

// const filePath = "./data/1-partition-brotli.parquet";
// const filePath = "./data/1-partition-gzip.parquet";
// const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/1-partition-snappy.parquet";
const filePath = "./data/1-partition-none.parquet";
// const filePath = "./data/1-partition-zstd.parquet";
// const filePath = './water-stress_rcp26and85_2020-2040-10.parquet'
// const filePath = './test.parquet'

async function fetchData() {
  let fileByteArray;
  try {
    let fetchResponse = await fetch(filePath);
    fileByteArray = new Uint8Array(await fetchResponse.arrayBuffer());
  } catch (fetchErr) {
    console.error("Fetch error: " + fetchErr);
  }

  console.log("Parquet data bytelength: " + fileByteArray.byteLength);

  const arrow_result_ipc_msg_bytes = wasm.read_parquet(fileByteArray);
  console.log("finished reading");
  window.data = arrow_result_ipc_msg_bytes;

  // var file = new Blob(data, { type: 'application/octet-stream' });
  // var a = document.createElement('a');
  // a.href = URL.createObjectURL(file);
  // a.download = 'data.arrow';
  // a.click();

  return arrow_result_ipc_msg_bytes;
}

async function main() {
  console.time("fetchData");
  const arrow_result_ipc_msg_bytes = await fetchData();
  console.timeEnd("fetchData");

  try {
    const record_batch_reader = arrow.RecordBatchReader.from(
      arrow_result_ipc_msg_bytes
    );
    for (const batch of record_batch_reader) {
      window.batch = batch;

      console.log("schema is: " + batch.schema);

      console.log("result rowcount: " + batch.data.length);
    }
  } catch (record_batch_reader_err) {
    console.error("problem with record_batch_reader: " + record_batch_reader_err);
  }

  console.log("end of js");
}

console.log("trigger fetch data");
main();
