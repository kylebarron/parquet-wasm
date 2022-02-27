//let arrow = require("./node_modules/apache-arrow/Arrow");
// import * as arrow from "./node_modules/apache-arrow/Arrow.es2015.min";
import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "read-parquet-browser";

window.arrow = arrow;

async function fetchData() {
  let fileByteArray;
  try {
    let fetchResponse = await fetch(
      "./water-stress_rcp26and85_2020-2040-10.parquet"
    );
    fileByteArray = new Uint8Array(await fetchResponse.arrayBuffer());
  } catch (fetchErr) {
    console.error("Fetch error: " + fetchErr);
  }

  console.log("Parquet data bytelength: " + fileByteArray.byteLength);

  const test = wasm.read_geo_physical_risk_parquet("water-stress", fileByteArray);
  console.log('finished reading')
  window.test = test;

  // var file = new Blob(test, { type: 'application/octet-stream' });
  // var a = document.createElement('a');
  // a.href = URL.createObjectURL(file);
  // a.download = 'data.arrow';
  // a.click();

}

/*
window.onload = async function() {
    console.log("window onload called...");

    //wasm.init();

    //await fetchData();

    document
        .getElementById("button-trigger-read-parquet")
        .addEventListener("click", async function() {
                console.log("the button was clicked");
                await fetchData();
            });

    console.log("button click event listener added");

    console.log("end window onload.");
}
*/

async function main() {
  console.time('fetchData')
  await fetchData();
  console.timeEnd('fetchData')

  console.time('filter')

  console.log("filtering for specific rcp...");
  let arrow_result_ipc_msg_bytes = wasm.find_for_rcp("water-stress", 2);
  console.log(
    "filtering for specific rcp complete, returned buffer byte count: " +
      arrow_result_ipc_msg_bytes.byteLength
  );
  console.timeEnd('filter')

  try {
    let record_batch_reader = arrow.RecordBatchStreamReader.from(
      arrow_result_ipc_msg_bytes
    );
    record_batch_reader.open();
    console.log("schema is: " + record_batch_reader.schema);
    let result_record_batch = record_batch_reader.next();
    console.log("result rowcount: " + result_record_batch.value.length);
  } catch (record_batch_reader_err) {
    console.log("problem with record_batch_reader: " + record_batch_reader_err);
  }

  console.log("end of js");
}

console.log("trigger fetch data");
main();
