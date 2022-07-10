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

  const recordBatchChunks = [];
  for (let i = 0; i < metadata.numRowGroups(); i++) {
    const arrowIpcBuffer = await wasm.readRowGroupAsync2(url, length, metadata.copy(), i);
    recordBatchChunks.push(...arrow.tableFromIPC(arrowIpcBuffer).batches);
  }

  const table = new arrow.Table(recordBatchChunks);
  window.table = table;
  console.log("table", table);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
