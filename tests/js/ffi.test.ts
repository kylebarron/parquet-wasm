import * as wasm from "../../pkg/node/parquet_wasm";
import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import {
  testArrowTablesEqual,
  readExpectedArrowData,
  temporaryServer,
} from "./utils";
import { parseRecordBatch } from "arrow-js-ffi";
import { it } from "vitest";

// Path from repo root
const dataDir = "tests/data";

// @ts-expect-error
const WASM_MEMORY: WebAssembly.Memory = wasm.__wasm.memory;

it("read via FFI", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiTable = wasm.readParquet(arr).intoFFI();

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
  testArrowTablesEqual(expectedTable, initialTable);
});

it("read file stream", async (t) => {
  const server = await temporaryServer();
  const listeningPort = server.addresses()[0].port;
  const rootUrl = `http://localhost:${listeningPort}`;

  const expectedTable = readExpectedArrowData();

  const url = `${rootUrl}/1-partition-brotli.parquet`;
  const stream = (await wasm.readParquetStream(
    url
  )) as unknown as wasm.RecordBatch[];
  const batches = [];
  for await (const table of stream) {
    const ffiTable = table.intoFFI();
    const recordBatch = parseRecordBatch(
      WASM_MEMORY.buffer,
      ffiTable.arrayAddr(),
      ffiTable.schemaAddr(),
      true
    );
    batches.push(recordBatch);
  }
  const initialTable = new arrow.Table(batches);
  testArrowTablesEqual(expectedTable, initialTable);
  await server.close();
});
