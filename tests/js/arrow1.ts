import * as test from "tape";
import * as wasm from "../../pkg/node/arrow1";
import { readFileSync } from "fs";
import { tableFromIPC, tableToIPC } from "apache-arrow";
import { testArrowTablesEqual, readExpectedArrowData } from "./utils";

// Path from repo root
const dataDir = "tests/data";
const testFiles = [
  "1-partition-brotli.parquet",
  "1-partition-gzip.parquet",
  // "1-partition-lz4.parquet",
  "1-partition-none.parquet",
  "1-partition-snappy.parquet",
  "1-partition-zstd.parquet",
  "2-partition-brotli.parquet",
  "2-partition-gzip.parquet",
  // "2-partition-lz4.parquet",
  "2-partition-none.parquet",
  "2-partition-snappy.parquet",
  "2-partition-zstd.parquet",
];

test("read file", async (t) => {
  const expectedTable = readExpectedArrowData();

  for (const testFile of testFiles) {
    const dataPath = `${dataDir}/${testFile}`;
    const arr = new Uint8Array(readFileSync(dataPath));
    const table = tableFromIPC(wasm.readParquet(arr));
    testArrowTablesEqual(t, expectedTable, table);
  }

  t.end();
});

test("read-write-read round trip (with writer properties)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr));

  const writerProperties = new wasm.WriterPropertiesBuilder().build();

  const parquetBuffer = wasm.writeParquet(
    tableToIPC(initialTable, "stream"),
    writerProperties
  );
  const table = tableFromIPC(wasm.readParquet(parquetBuffer));

  testArrowTablesEqual(t, initialTable, table);
  t.end();
});

test("read-write-read round trip (no writer properties provided)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr));

  const parquetBuffer = wasm.writeParquet(tableToIPC(initialTable, "stream"));
  const table = tableFromIPC(wasm.readParquet(parquetBuffer));

  testArrowTablesEqual(t, initialTable, table);
  t.end();
});

test("error produced trying to read file with arrayBuffer", (t) => {
  const arrayBuffer = new ArrayBuffer(10);
  try {
    // @ts-expect-error input should be Uint8Array
    wasm.readParquet(arrayBuffer);
  } catch (err) {
    t.ok(err instanceof Error, "err expected to be an Error");
    t.equals(err.message, "Empty input provided or not a Uint8Array.", "Expected error message");
  }

  t.end();
});
