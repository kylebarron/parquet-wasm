import * as arrow from "@apache-arrow/es2015-cjs/Arrow.dom";
import * as wasm from "parquet-wasm/bundler";

wasm.setPanicHook();

window.wasm = wasm;
// const url = 'https://raw.githubusercontent.com/opengeospatial/geoparquet/main/examples/example.parquet';
const url =
  "https://raw.githubusercontent.com/kylebarron/parquet-wasm/main/tests/data/2-partition-brotli.parquet";
window.url = url;
ReadableStream.prototype[Symbol.asyncIterator] = async function* () {
  const reader = this.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
};
async function readParquetFile() {
  const blobResult = (await fetch(url)).blob();
  // a bit pointless, but definitely a file.
  const simulatedFile = new File([blobResult], "2-partition-brotli.parquet", {
    type: "application/vnd.apache.parquet",
  });
  const instance = await new wasmArrow1.AsyncParquetLocalFile(simulatedFile);
  for await (const chunk of instance.stream()) {
    console.log(chunk);
    console.log(chunk.length);
    chunk.free();
  }
}
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
  console.log("tables", tables);

  const table = new arrow.Table(tables);
  window.table = table;
  console.log("table", table);
  console.log("file IO");
  await readParquetFile();
  console.log("file IO");
  console.log("end of js");
}

console.log("trigger fetch data");
main();
