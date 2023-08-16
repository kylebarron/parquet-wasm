import * as test from "tape";
import * as wasm from "../../pkg/node/arrow2";
import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import { testArrowTablesEqual, readExpectedArrowData } from "./utils";
import { parseRecordBatch } from "arrow-js-ffi";

// Path from repo root
const dataDir = "tests/data";

// @ts-expect-error
const WASM_MEMORY: WebAssembly.Memory = wasm.__wasm.memory;

test("read via FFI", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiTable = wasm.readParquetFFI(arr);

  const batches: arrow.RecordBatch[] = [];
  for (let i = 0; i < ffiTable.numBatches(); i++) {
    const recordBatch = parseRecordBatch(
      WASM_MEMORY.buffer,
      ffiTable.arrayAddr(i),
      ffiTable.schemaAddr(),
      true
    );
    batches.push(recordBatch);
  }

  const initialTable = new arrow.Table(batches);
  testArrowTablesEqual(t, expectedTable, initialTable);
  t.end();
});
