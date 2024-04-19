import * as wasm from "../../pkg/node/parquet_wasm";
import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import {
  testArrowTablesEqual,
  readExpectedArrowData,
  temporaryServer,
} from "./utils";
import { parseTable, parseRecordBatch } from "arrow-js-ffi";
import { it } from "vitest";

// Path from repo root
const dataDir = "tests/data";

const WASM_MEMORY = wasm.wasmMemory();

it("read via FFI", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiTable = wasm.readParquet(arr).intoFFI();

  const table = parseTable(
    WASM_MEMORY.buffer,
    ffiTable.arrayAddrs(),
    ffiTable.schemaAddr()
  );
  testArrowTablesEqual(expectedTable, table);
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
  for await (const wasmRecordBatch of stream) {
    const ffiRecordBatch = wasmRecordBatch.intoFFI();
    const recordBatch = parseRecordBatch(
      WASM_MEMORY.buffer,
      ffiRecordBatch.arrayAddr(),
      ffiRecordBatch.schemaAddr(),
      true
    );
    batches.push(recordBatch);
  }
  const initialTable = new arrow.Table(batches);
  testArrowTablesEqual(expectedTable, initialTable);
  await server.close();
});
