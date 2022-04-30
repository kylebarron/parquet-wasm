import * as wasm from "parquet-wasm";

wasm.setPanicHook();
window.wasm = wasm;

async function main() {
  console.log('hello world');
  const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
  // const test1 = await wasm.getContentLength(url);
  // const test2 = wasm.read_parquet_metadata_async_arrow1(url);
  const test3 = await wasm.read_parquet_metadata_async(url);
  console.log('test3', test3);

  // await wasm.readParquet
  // wasm.

  // const test = await wasm.run('kylebarron/parquet-wasm');
  // console.log('test', test);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
