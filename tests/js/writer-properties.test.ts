import {
  RecordBatch,
  Table,
  tableFromArrays,
  tableFromIPC,
  tableToIPC,
} from "apache-arrow";
import { readFileSync } from "fs";
import { describe, expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm";
import { testArrowTablesEqual } from "./utils";

function writeParquet(table: Table, props: wasm.WriterProperties): Uint8Array {
  return wasm.writeParquet(
    wasm.Table.fromIPCStream(tableToIPC(table, "stream")),
    props,
  );
}

async function rowGroupRowCounts(bytes: Uint8Array): Promise<number[]> {
  const file = await wasm.ParquetFile.fromFile(new Blob([bytes.slice()]));
  try {
    const meta = file.metadata();
    const groups = [];
    for (let i = 0; i < meta.numRowGroups(); i++) {
      groups.push(meta.rowGroup(i).numRows());
    }
    return groups;
  } finally {
    file.free();
  }
}

describe("WriterPropertiesBuilder row group limits", () => {
  it("keeps a small table in one row group when limits are left at defaults", async () => {
    const table = tableFromArrays({
      id: Int32Array.from([1, 2, 3, 4]),
    });
    const bytes = writeParquet(
      table,
      new wasm.WriterPropertiesBuilder().build(),
    );
    const groups = await rowGroupRowCounts(bytes);
    expect(groups).toEqual([4]);
  });

  it("splits by row count when setMaxRowGroupSize is called", async () => {
    const table = tableFromArrays({
      id: Int32Array.from([1, 2, 3, 4, 5]),
    });
    const bytes = writeParquet(
      table,
      new wasm.WriterPropertiesBuilder().setMaxRowGroupSize(2).build(),
    );
    const groups = await rowGroupRowCounts(bytes);
    expect(groups).toEqual([2, 2, 1]);
  });

  it("splits by encoded size when setMaxRowGroupBytes is called", async () => {
    // A first oversized batch is written whole, so this uses several small ones.
    const chunks = Array.from({ length: 10 }, (_, batch) =>
      tableFromArrays({
        s: Array.from({ length: 10 }, (_, i) =>
          String(batch * 10 + i).padStart(100, "0")),
      }));
    const schema = chunks[0].schema;
    const table = new Table(
      schema,
      chunks.map((t) => new RecordBatch(schema, t.batches[0].data)),
    );
    // Same payload, no byte cap → one group
    const defaultBytes = writeParquet(
      table,
      new wasm.WriterPropertiesBuilder()
        .setCompression(wasm.Compression.UNCOMPRESSED)
        .build(),
    );
    expect(await rowGroupRowCounts(defaultBytes)).toEqual([100]);

    const bytes = writeParquet(
      table,
      new wasm.WriterPropertiesBuilder()
        .setCompression(wasm.Compression.UNCOMPRESSED)
        .setMaxRowGroupBytes(3500)
        .build(),
    );
    const groups = await rowGroupRowCounts(bytes);
    expect(groups.reduce((n, rows) => n + rows, 0)).toBe(100);
    expect(groups.length).toBeGreaterThan(1);
  });
});

describe("WriterPropertiesBuilder content-defined chunking", () => {
  // Distinct strings so that pages are large enough to be split into chunks.
  const table = tableFromArrays({
    text: Array.from({ length: 4096 }, (_, i) => `row-${i}-`.repeat(8)),
  });

  function writeWith(builder: wasm.WriterPropertiesBuilder): Uint8Array {
    return writeParquet(
      table,
      builder
        .setCompression(wasm.Compression.UNCOMPRESSED)
        .setDictionaryEnabled(false)
        .build(),
    );
  }

  function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
    return Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;
  }

  it("round trips a parquet file", () => {
    const arr = new Uint8Array(
      readFileSync("tests/data/1-partition-snappy.parquet"),
    );
    const expected = tableFromIPC(wasm.readParquet(arr).intoIPCStream());
    const bytes = wasm.writeParquet(
      wasm.readParquet(arr),
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking().build(),
    );
    testArrowTablesEqual(
      expected,
      tableFromIPC(wasm.readParquet(bytes).intoIPCStream()),
    );
  });

  it("changes the page layout when setContentDefinedChunking is called", () => {
    const defaultBytes = writeWith(new wasm.WriterPropertiesBuilder());
    const bytes = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        minChunkSize: 1024,
        maxChunkSize: 4096,
      }),
    );
    expect(equalBytes(bytes, defaultBytes)).toBe(false);
    testArrowTablesEqual(
      table,
      tableFromIPC(wasm.readParquet(bytes).intoIPCStream()),
    );
  });

  it("uses upstream defaults for omitted options", () => {
    const defaults = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(),
    );
    const explicit = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        minChunkSize: 256 * 1024,
        maxChunkSize: 1024 * 1024,
        normLevel: 0,
      }),
    );
    const partial = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        maxChunkSize: 1024 * 1024,
      }),
    );
    expect(equalBytes(explicit, defaults)).toBe(true);
    expect(equalBytes(partial, defaults)).toBe(true);
  });

  it.each([
    { minChunkSize: 0 },
    { minChunkSize: 1024, maxChunkSize: 1024 },
    { minChunkSize: 4096, maxChunkSize: 1024 },
    { minChunkSize: -1 },
  ])("throws on invalid options without breaking the module: %j", (options) => {
    expect(() =>
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(options),
    ).toThrow();
    expect(writeWith(new wasm.WriterPropertiesBuilder()).length).toBeGreaterThan(
      0,
    );
  });
});
