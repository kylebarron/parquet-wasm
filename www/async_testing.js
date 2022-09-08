import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm/arrow2";

wasm.setPanicHook();
window.wasm = wasm;
// const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
const url =
  "https://raw.githubusercontent.com/kylebarron/parquet-wasm/main/tests/data/2-partition-brotli.parquet";
window.url = url;

async function main() {
  console.log("hello world");
  const resp = await fetch(url, { method: "HEAD" });
  const length = parseInt(resp.headers.get("Content-Length"));

  const metadata = await wasm.readMetadataAsync(url, length);
  const arrowSchema = metadata.arrowSchema();

  // Read all batches from the file in parallel
  const promises = [];
  for (let i = 0; i < metadata.numRowGroups(); i++) {
    const rowGroupPromise = wasm.readRowGroupAsync(
      url,
      metadata.copy().rowGroup(i),
      arrowSchema.copy()
    );
    promises.push(rowGroupPromise);
  }

  const recordBatchChunks = await Promise.all(promises);
  const tables = recordBatchChunks.map(arrow.tableFromIPC);
  console.log('tables', tables)

  const table = new arrow.Table(tables);
  window.table = table;
  console.log("table", table);

  console.log("end of js");
}

console.log("trigger fetch data");
main();
