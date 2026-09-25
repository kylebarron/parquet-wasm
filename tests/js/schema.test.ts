import { readFileSync } from "node:fs";
import * as arrow from "apache-arrow";
import { parseSchema } from "arrow-js-ffi";
import { expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm.js";
import { readExpectedArrowData } from "./utils.js";

// Path from repo root
const dataDir = "tests/data";

const WASM_MEMORY = wasm.wasmMemory();

it("read schema via FFI", async () => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiSchema = wasm.readSchema(arr).intoFFI();

  const schema = parseSchema(WASM_MEMORY.buffer, ffiSchema.addr());

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length,
  );
});

it("read schema via IPC", async () => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ipcSchema = wasm.readSchema(arr).intoIPCStream();

  const schema = arrow.tableFromIPC(ipcSchema).schema;

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length,
  );
});
