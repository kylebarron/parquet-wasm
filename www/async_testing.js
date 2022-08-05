import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm/bundler/arrow2";

wasm.setPanicHook();
window.wasm = wasm;
// const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
const url = 'https://raw.githubusercontent.com/kylebarron/parquet-wasm/main/tests/data/2-partition-brotli.parquet';
window.url = url;

async function main() {
  console.log('hello world');
  const resp = await fetch(url, {method: 'HEAD'});
  const length = parseInt(resp.headers.get('Content-Length'));

  const metadata = await wasm.readMetadataAsync2(url, length);

  // Read all batches from the file in parallel
  const promises = [];
  for (let i = 0; i < metadata.numRowGroups(); i++) {
    const rowGroupPromise = wasm.readRowGroupAsync2(url, length, metadata.copy(), i);
    promises.push(rowGroupPromise);
  }

  const recordBatchChunks = await Promise.all(promises);

  const table = new arrow.Table(recordBatchChunks);
  window.table = table;
  console.log("table", table);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
