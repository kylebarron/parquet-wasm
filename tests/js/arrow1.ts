import * as test from "tape";
import * as wasm from "../../pkg/node";
import { readFileSync } from "fs";
import { tableFromIPC, tableToIPC, Table } from "apache-arrow";

// Path from repo root
const dataDir = "tests/data";
const testFiles = [
  "1-partition-brotli.parquet",
  "1-partition-gzip.parquet",
  // "1-partition-lz4.parquet",
  "1-partition-none.parquet",
  "1-partition-snappy.parquet",
  // "1-partition-zstd.parquet",
  "2-partition-brotli.parquet",
  "2-partition-gzip.parquet",
  // "2-partition-lz4.parquet",
  "2-partition-none.parquet",
  "2-partition-snappy.parquet",
  // "2-partition-zstd.parquet",
];

function testArrow(t: test.Test, table: Table) {
  t.equals(table.numRows, 4, "correct number of rows");
  t.deepEquals(
    table.schema.fields.map((f) => f.name),
    ["str", "uint8", "int32", "bool"],
    "correct column names"
  );
}

test("read file", async (t) => {
  for (const testFile of testFiles) {
    const dataPath = `${dataDir}/${testFile}`;
    const buffer = readFileSync(dataPath);
    const arr = new Uint8Array(buffer);
    const table = tableFromIPC(wasm.readParquet(arr));
    testArrow(t, table);
  }

  t.end();
});

test("write and read file", async (t) => {
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

  testArrow(t, table);
  t.end();
});
