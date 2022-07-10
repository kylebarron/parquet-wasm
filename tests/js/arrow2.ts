import * as test from "tape";
import * as wasm from "../../pkg/node/arrow2";
import { readFileSync } from "fs";
import { RecordBatch, Table, tableFromIPC, tableToIPC } from "apache-arrow";
import { testArrowTablesEqual, readExpectedArrowData } from "./utils";

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

test("read file", async (t) => {
  const expectedTable = readExpectedArrowData();

  for (const testFile of testFiles) {
    const dataPath = `${dataDir}/${testFile}`;
    const arr = new Uint8Array(readFileSync(dataPath));
    const table = tableFromIPC(wasm.readParquet2(arr));
    testArrowTablesEqual(t, expectedTable, table);
  }

  t.end();
});

test("read-write-read round trip (with writer properties)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet2(arr));

  const writerProperties = new wasm.WriterPropertiesBuilder().build();

  const parquetBuffer = wasm.writeParquet2(
    tableToIPC(initialTable, "file"),
    writerProperties
  );
  const table = tableFromIPC(wasm.readParquet2(parquetBuffer));

  testArrowTablesEqual(t, initialTable, table);
  t.end();
});

test("read-write-read round trip (no writer properties provided)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet2(arr));

  const parquetBuffer = wasm.writeParquet2(tableToIPC(initialTable, "file"));
  const table = tableFromIPC(wasm.readParquet2(parquetBuffer));

  testArrowTablesEqual(t, initialTable, table);
  t.end();
});

test("error produced trying to read file with arrayBuffer", (t) => {
  const arrayBuffer = new ArrayBuffer(10);
  try {
    // @ts-expect-error input should be Uint8Array
    wasm.readParquet2(arrayBuffer);
  } catch (err) {
    t.equals(err, "Empty input provided or not a Uint8Array.");
  }

  t.end();
});

test("iterate over row groups", (t) => {
  const dataPath = `${dataDir}/2-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const fileMetaData = wasm.readMetadata2(arr);

  const chunks: RecordBatch[] = [];
  for (let i = 0; i < fileMetaData.numRowGroups(); i++) {
    let arrowIpcBuffer = wasm.readRowGroup2(arr, fileMetaData, i);
    chunks.push(...tableFromIPC(arrowIpcBuffer).batches);
  }

  const table = new Table(chunks);
  const expectedTable = readExpectedArrowData();
  testArrowTablesEqual(t, expectedTable, table);

  t.end();
});
