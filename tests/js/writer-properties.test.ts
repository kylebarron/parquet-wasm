import {
  RecordBatch,
  Table,
  tableFromArrays,
  tableFromIPC,
  tableToIPC,
  Utf8,
  vectorFromArray,
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
  // Deterministic pseudo-random plain (non-dictionary) strings, so the rolling hash finds
  // chunk boundaries.
  const makeTable = (rows: number, parts = 24) => {
    let seed = 42;
    const next = () => {
      seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
      return seed.toString(36);
    };
    const values = Array.from({ length: rows }, () =>
      Array.from({ length: parts }, next).join(""),
    );
    return tableFromArrays({ text: vectorFromArray(values, new Utf8()) });
  };
  const table = makeTable(512);
  const small = { minChunkSize: 1024, maxChunkSize: 4096 };

  function writeWith(
    builder: wasm.WriterPropertiesBuilder,
    data: Table = table,
  ): Uint8Array {
    return writeParquet(
      data,
      builder
        .setCompression(wasm.Compression.UNCOMPRESSED)
        .setDictionaryEnabled(false)
        .build(),
    );
  }

  function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
    return Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;
  }

  it("round trips a real file with CDC enabled", () => {
    // Too small to hit a chunk boundary; this checks CDC doesn't break writing mixed types.
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
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(small),
    );
    expect(equalBytes(bytes, defaultBytes)).toBe(false);
    testArrowTablesEqual(
      table,
      tableFromIPC(wasm.readParquet(bytes).intoIPCStream()),
    );
  });

  it("passes normLevel through", () => {
    const norm0 = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(small),
    );
    const norm1 = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        ...small,
        normLevel: 1,
      }),
    );
    expect(equalBytes(norm0, norm1)).toBe(false);
  });

  it("uses upstream defaults for omitted options", () => {
    // ~640 KB of seeded data that default CDC splits into 2 pages (1 page without CDC).
    const large = makeTable(1024, 96);
    const disabled = writeWith(new wasm.WriterPropertiesBuilder(), large);
    const defaults = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(),
      large,
    );
    const explicit = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        minChunkSize: 256 * 1024,
        maxChunkSize: 1024 * 1024,
        normLevel: 0,
      }),
      large,
    );
    const partial = writeWith(
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking({
        maxChunkSize: 1024 * 1024,
      }),
      large,
    );
    expect(equalBytes(defaults, disabled)).toBe(false);
    expect(equalBytes(explicit, defaults)).toBe(true);
    expect(equalBytes(partial, defaults)).toBe(true);
  });

  it("throws a readable error on invalid options instead of panicking", () => {
    const set = (options: wasm.ContentDefinedChunkingOptions) => () =>
      new wasm.WriterPropertiesBuilder().setContentDefinedChunking(options);
    // Matching the message rules out an upstream panic, which surfaces as "unreachable".
    expect(set({ minChunkSize: 0 })).toThrow(/minChunkSize must be greater than 0/);
    expect(set({ minChunkSize: 4096, maxChunkSize: 1024 })).toThrow(
      /maxChunkSize must be greater than minChunkSize/,
    );
  });
});
