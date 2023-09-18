import * as wasm from "../../pkg/node/arrow2";
import { readFileSync } from "fs";
import { RecordBatch, Table, tableFromIPC, tableToIPC } from "apache-arrow";
import { testArrowTablesEqual, readExpectedArrowData } from "./utils";
import { describe, it, expect } from "vitest";

// Path from repo root
const dataDir = "tests/data";
const testFiles = [
  "1-partition-brotli.parquet",
  "1-partition-gzip.parquet",
  "1-partition-lz4.parquet",
  "1-partition-none.parquet",
  "1-partition-snappy.parquet",
  "1-partition-zstd.parquet",
  "2-partition-brotli.parquet",
  "2-partition-gzip.parquet",
  "2-partition-lz4.parquet",
  "2-partition-none.parquet",
  "2-partition-snappy.parquet",
  "2-partition-zstd.parquet",
];

describe("read file", () => {
  const expectedTable = readExpectedArrowData();
  for (const testFile of testFiles) {
    it(testFile, () => {
      const dataPath = `${dataDir}/${testFile}`;
      const arr = new Uint8Array(readFileSync(dataPath));
      const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());
      testArrowTablesEqual(expectedTable, table);
    });
  }
});

it("read-write-read round trip (with writer properties)", () => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const writerProperties = new wasm.WriterPropertiesBuilder().build();

  const parquetBuffer = wasm.writeParquet(
    wasm.Table.fromIPC(tableToIPC(initialTable, "file")),
    writerProperties
  );
  const table = tableFromIPC(wasm.readParquet(parquetBuffer).intoIPCStream());

  testArrowTablesEqual(initialTable, table);
});

it("read-write-read round trip (no writer properties provided)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const parquetBuffer = wasm.writeParquet(
    wasm.Table.fromIPC(tableToIPC(initialTable, "file"))
  );
  const table = tableFromIPC(wasm.readParquet(parquetBuffer).intoIPCStream());

  testArrowTablesEqual(initialTable, table);
});

it("error produced trying to read file with arrayBuffer", (t) => {
  const arrayBuffer = new ArrayBuffer(10);
  try {
    // @ts-expect-error input should be Uint8Array
    wasm.readParquet(arrayBuffer);
  } catch (err) {
    expect(err instanceof Error, "err expected to be an Error").toBeTruthy();
    expect(err.message, "Expected error message").toStrictEqual(
      "Empty input provided or not a Uint8Array."
    );
  }
});

it("iterate over row groups", (t) => {
  const dataPath = `${dataDir}/2-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const fileMetaData = wasm.readMetadata(arr);
  const arrowSchema = fileMetaData.arrowSchema();

  const chunks: RecordBatch[] = [];
  for (let i = 0; i < fileMetaData.numRowGroups(); i++) {
    let arrowIpcBuffer = wasm
      .readRowGroup(arr, arrowSchema, fileMetaData.rowGroup(i))
      .intoIPCStream();
    chunks.push(...tableFromIPC(arrowIpcBuffer).batches);
  }

  const table = new Table(chunks);
  const expectedTable = readExpectedArrowData();
  testArrowTablesEqual(expectedTable, table);
});
