import * as wasm from "parquet-wasm";

window.wasm = wasm;

async function main() {
  const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
  // const test1 = await wasm.getContentLength(url);
  const test2 = await wasm.makeRangeRequest(url);


  // const test = await wasm.run('kylebarron/parquet-wasm');
  // console.log('test', test);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
