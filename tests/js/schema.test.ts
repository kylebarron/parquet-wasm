import * as wasm from "../../pkg/node/parquet_wasm";
import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import { readExpectedArrowData, extractFooterBytes } from "./utils";
import { parseSchema } from "arrow-js-ffi";
import { it, expect } from "vitest";

// Path from repo root
const dataDir = "tests/data";

const WASM_MEMORY = wasm.wasmMemory();

it("read schema via FFI", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiSchema = wasm.readSchema(arr).intoFFI();

  const schema = parseSchema(WASM_MEMORY.buffer, ffiSchema.addr());

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length
  );
});

it("read schema via IPC", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ipcSchema = wasm.readSchema(arr).intoIPCStream();

  const schema = arrow.tableFromIPC(ipcSchema).schema;

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length
  );
});

it("read metadata from full file bytes", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  // TODO: test with footer bytes alone as well
  const metadata = wasm.readMetadata(arr);

  // Convert the parquet file buffer from readFileSync to a Blob
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  // Use the ParquetJS library to read the metadata from the Blob
  // Note that ParquetJS currently only supports reading from a file or a Blob, not from a buffer directly
  // So we have to use a Blob here
  // See
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(expectedMetadata.fileMetadata().createdBy());
  expect(metadata.fileMetadata().numRows()).toStrictEqual(expectedMetadata.fileMetadata().numRows());
  expect(metadata.fileMetadata().version()).toStrictEqual(expectedMetadata.fileMetadata().version());
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(expectedMetadata.numRowGroups());
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(expectedMetadata.rowGroup(0).numRows());
});

it("read metadata from footer bytes only", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  // TODO: test with footer bytes alone as well
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  // Use the ParquetJS library to read the metadata from the Blob
  // Note that ParquetJS currently only supports reading from a file or a Blob, not from a buffer directly
  // So we have to use a Blob here
  // See
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(expectedMetadata.fileMetadata().createdBy());
  expect(metadata.fileMetadata().numRows()).toStrictEqual(expectedMetadata.fileMetadata().numRows());
  expect(metadata.fileMetadata().version()).toStrictEqual(expectedMetadata.fileMetadata().version());
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(expectedMetadata.numRowGroups());
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(expectedMetadata.rowGroup(0).numRows());
});
