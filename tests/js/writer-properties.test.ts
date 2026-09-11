import { RecordBatch, Table, tableFromArrays, tableToIPC } from "apache-arrow";
import { describe, expect, it } from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm";

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
