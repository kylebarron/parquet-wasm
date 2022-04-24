import * as wasm from "parquet-wasm";

window.wasm = wasm;

async function main() {
  const test = await wasm.run('kylebarron/parquet-wasm');
  console.log('test', test);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
