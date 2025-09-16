import { DataType, tableFromIPC, tableToIPC } from "apache-arrow";
import { readFileSync } from "fs";
import { describe, expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm";
import {
  readExpectedArrowData,
  temporaryServer,
  testArrowTablesEqual,
} from "./utils";

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

describe("read file", async (t) => {
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

it("read-write-read round trip (with writer properties)", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const writerProperties = new wasm.WriterPropertiesBuilder().build();

  const parquetBuffer = wasm.writeParquet(
    wasm.Table.fromIPCStream(tableToIPC(initialTable, "stream")),
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
    wasm.Table.fromIPCStream(tableToIPC(initialTable, "stream"))
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

it("reads empty file", async (t) => {
  const dataPath = `${dataDir}/empty.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  expect(table.schema.fields.length).toStrictEqual(0);
  expect(table.numRows).toStrictEqual(0);
  expect(table.numCols).toStrictEqual(0);
  // console.log("empty table schema", table.schema);
});

it("read stream-write stream-read stream round trip (no writer properties provided)", async (t) => {
  const server = await temporaryServer();
  const listeningPort = server.addresses()[0].port;
  const rootUrl = `http://localhost:${listeningPort}`;

  const expectedTable = readExpectedArrowData();

  const url = `${rootUrl}/1-partition-brotli.parquet`;
  const originalStream = await wasm.readParquetStream(url);

  const stream = await wasm.transformParquetStream(originalStream);
  const accumulatedBuffer = new Uint8Array(
    await new Response(stream).arrayBuffer()
  );
  const roundtripTable = tableFromIPC(
    wasm.readParquet(accumulatedBuffer).intoIPCStream()
  );

  testArrowTablesEqual(expectedTable, roundtripTable);
  await server.close();
});

it("read string view file", async (t) => {
  const dataPath = `${dataDir}/string_view.parquet`;
  const arr = new Uint8Array(readFileSync(dataPath));
  const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const stringCol = table.getChild("string_view")!;
  expect(DataType.isUtf8(stringCol.type)).toBeTruthy();

  const binaryCol = table.getChild("binary_view")!;
  expect(DataType.isBinary(binaryCol.type)).toBeTruthy();
});
