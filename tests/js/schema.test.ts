import { readFileSync } from "node:fs";
import * as arrow from "apache-arrow";
import { parseSchema } from "arrow-js-ffi";
import { expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm.js";
import { extractFooterBytes, readExpectedArrowData } from "./utils.js";

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

it("read metadata from full file bytes", async () => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const metadata = wasm.readMetadata(arr);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  // Test against the existing ParquetFile.metadata method.
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(
    expectedMetadata.fileMetadata().createdBy(),
  );
  expect(metadata.fileMetadata().numRows()).toStrictEqual(
    expectedMetadata.fileMetadata().numRows(),
  );
  expect(metadata.fileMetadata().version()).toStrictEqual(
    expectedMetadata.fileMetadata().version(),
  );
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(
    expectedMetadata.numRowGroups(),
  );
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(
    expectedMetadata.rowGroup(0).numRows(),
  );
});

it("read metadata from footer bytes only", async () => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  // Test against the existing ParquetFile.metadata method.
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(
    expectedMetadata.fileMetadata().createdBy(),
  );
  expect(metadata.fileMetadata().numRows()).toStrictEqual(
    expectedMetadata.fileMetadata().numRows(),
  );
  expect(metadata.fileMetadata().version()).toStrictEqual(
    expectedMetadata.fileMetadata().version(),
  );
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(
    expectedMetadata.numRowGroups(),
  );
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(
    expectedMetadata.rowGroup(0).numRows(),
  );
});
