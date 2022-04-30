import * as wasm from "parquet-wasm";

wasm.setPanicHook();
window.wasm = wasm;
const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
window.url = url;

async function main() {
  console.log('hello world');
  const parquetFile = await new wasm.AsyncParquetFile(url);
  window.parquetFile = parquetFile;
  console.log('parquetFile', parquetFile);
  console.log('content length', parquetFile.content_length());

  // await wasm.readParquet
  // wasm.

  // const test = await wasm.run('kylebarron/parquet-wasm');
  // console.log('test', test);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
