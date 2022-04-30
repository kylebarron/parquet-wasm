import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm";

wasm.setPanicHook();
window.wasm = wasm;
const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
window.url = url;

async function main() {
  console.log('hello world');
  const parquetFile = await new wasm.AsyncParquetFile(url);
  console.log('parquetFile', parquetFile);
  console.log('content length', parquetFile.content_length());
  window.parquetFile = parquetFile;
  const arrow_ipc_bytes = await parquetFile.read_row_group(0);

  const table = arrow.tableFromIPC(arrow_ipc_bytes);
  window.table = table;
  console.log("table", table);

  // await wasm.readParquet
  // wasm.

  // const test = await wasm.run('kylebarron/parquet-wasm');
  // console.log('test', test);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
