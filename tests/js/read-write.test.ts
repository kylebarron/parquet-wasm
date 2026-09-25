import { readFileSync } from "node:fs";
import { DataType, tableFromIPC, tableToIPC } from "apache-arrow";
import { describe, expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm.js";
import {
  readExpectedArrowData,
  temporaryServer,
  testArrowTablesEqual,
} from "./utils.js";

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

describe("read file", async () => {
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

it("read-write-read round trip (with writer properties)", async () => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const writerProperties = new wasm.WriterPropertiesBuilder().build();

  const parquetBuffer = wasm.writeParquet(
    wasm.Table.fromIPCStream(tableToIPC(initialTable, "stream")),
    writerProperties,
  );
  const table = tableFromIPC(wasm.readParquet(parquetBuffer).intoIPCStream());

  testArrowTablesEqual(initialTable, table);
});

it("read-write-read round trip (no writer properties provided)", async () => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const initialTable = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  const parquetBuffer = wasm.writeParquet(
    wasm.Table.fromIPCStream(tableToIPC(initialTable, "stream")),
  );
  const table = tableFromIPC(wasm.readParquet(parquetBuffer).intoIPCStream());

  testArrowTablesEqual(initialTable, table);
});

it("error produced trying to read file with arrayBuffer", () => {
  const arrayBuffer = new ArrayBuffer(10);
  try {
    // @ts-expect-error input should be Uint8Array
    wasm.readParquet(arrayBuffer);
  } catch (err) {
    expect(err instanceof Error, "err expected to be an Error").toBeTruthy();
    expect(err.message, "Expected error message").toStrictEqual(
      "Empty input provided or not a Uint8Array.",
    );
  }
});

it("reads empty file", async () => {
  const dataPath = `${dataDir}/empty.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

  expect(table.schema.fields.length).toStrictEqual(0);
  expect(table.numRows).toStrictEqual(0);
  expect(table.numCols).toStrictEqual(0);
  // console.log("empty table schema", table.schema);
});

it("read stream-write stream-read stream round trip (no writer properties provided)", async () => {
  const server = await temporaryServer();
  const listeningPort = server.addresses()[0].port;
  const rootUrl = `http://localhost:${listeningPort}`;

  const expectedTable = readExpectedArrowData();

  const url = `${rootUrl}/1-partition-brotli.parquet`;
  const originalStream = await wasm.readParquetStream(url);

  const stream = await wasm.transformParquetStream(originalStream);
  const accumulatedBuffer = new Uint8Array(
    await new Response(stream).arrayBuffer(),
  );
  const roundtripTable = tableFromIPC(
    wasm.readParquet(accumulatedBuffer).intoIPCStream(),
  );

  testArrowTablesEqual(expectedTable, roundtripTable);
  await server.close();
});

describe("read string view file", async () => {
  it("synchronous read", async () => {
    const dataPath = `${dataDir}/string_view.parquet`;
    const arr = new Uint8Array(readFileSync(dataPath));
    const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());

    const stringCol = table.getChild("string_view")!;
    expect(DataType.isUtf8(stringCol.type)).toBeTruthy();

    const binaryCol = table.getChild("binary_view")!;
    expect(DataType.isBinary(binaryCol.type)).toBeTruthy();
  });

  it("asynchronous read", async () => {
    const server = await temporaryServer();
    const listeningPort = server.addresses()[0].port;
    const rootUrl = `http://localhost:${listeningPort}`;

    const url = `${rootUrl}/string_view.parquet`;
    const file = await wasm.ParquetFile.fromUrl(url);
    const wasmTable = await file.read();
    const jsTable = tableFromIPC(wasmTable.intoIPCStream());

    const stringCol = jsTable.getChild("string_view")!;
    expect(DataType.isUtf8(stringCol.type)).toBeTruthy();

    const binaryCol = jsTable.getChild("binary_view")!;
    expect(DataType.isBinary(binaryCol.type)).toBeTruthy();

    await server.close();
  });
});

// Regression tests for https://github.com/kylebarron/parquet-wasm/issues/810, where the projected
// record batches were paired with the schema of the unprojected file.
describe("read projected columns", async () => {
  it("returns only the requested columns", async () => {
    const server = await temporaryServer();
    const listeningPort = server.addresses()[0].port;
    const rootUrl = `http://localhost:${listeningPort}`;

    const url = `${rootUrl}/2-partition-brotli.parquet`;
    const file = await wasm.ParquetFile.fromUrl(url);
    const wasmTable = await file.read({ columns: ["str", "int32"] });
    const jsTable = tableFromIPC(wasmTable.intoIPCStream());

    expect(jsTable.schema.fields.map((field) => field.name)).toStrictEqual([
      "str",
      "int32",
    ]);
    expect(jsTable.numRows).toStrictEqual(4);
    expect(jsTable.getChild("str")!.toJSON()).toStrictEqual([
      "a",
      "b",
      "c",
      "d",
    ]);
    expect(jsTable.getChild("int32")!.toJSON()).toStrictEqual([
      0, -2147483638, 2147483637, 1,
    ]);

    await server.close();
  });

  it("returns only the requested columns of the requested row groups", async () => {
    const server = await temporaryServer();
    const listeningPort = server.addresses()[0].port;
    const rootUrl = `http://localhost:${listeningPort}`;

    const url = `${rootUrl}/2-partition-brotli.parquet`;
    const file = await wasm.ParquetFile.fromUrl(url);
    const wasmTable = await file.read({
      columns: ["str", "int32"],
      rowGroups: [1],
    });
    const jsTable = tableFromIPC(wasmTable.intoIPCStream());

    expect(jsTable.schema.fields.map((field) => field.name)).toStrictEqual([
      "str",
      "int32",
    ]);
    expect(jsTable.numRows).toStrictEqual(2);
    expect(jsTable.getChild("str")!.toJSON()).toStrictEqual(["c", "d"]);
    expect(jsTable.getChild("int32")!.toJSON()).toStrictEqual([2147483637, 1]);

    await server.close();
  });
});

it("rewrites ListView to List", () => {
  const arr = new Uint8Array(readFileSync(`${dataDir}/list_view.parquet`));
  const table = tableFromIPC(wasm.readParquet(arr).intoIPCStream());
  expect(DataType.isList(table.getChild("list_view")!.type)).toBeTruthy();
});

// https://github.com/kylebarron/parquet-wasm/issues/522
it("writeParquet frees its table and writer properties", () => {
  const table = wasm.readParquet(
    new Uint8Array(readFileSync(`${dataDir}/1-partition-snappy.parquet`)),
  );
  const writerProperties = new wasm.WriterPropertiesBuilder().build();
  wasm.writeParquet(table, writerProperties);

  // A pointer of 0 means wasm-bindgen has already freed the object. `__wbg_ptr` is internal,
  // so it isn't in the generated types.
  const ptr = (obj: object) => (obj as { __wbg_ptr: number }).__wbg_ptr;
  expect(ptr(table)).toBe(0);
  expect(ptr(writerProperties)).toBe(0);
  expect(() => table.free()).toThrow(/null pointer passed to rust/);
  expect(() => wasm.writeParquet(table)).toThrow(
    /Attempt to use a moved value/,
  );
});
