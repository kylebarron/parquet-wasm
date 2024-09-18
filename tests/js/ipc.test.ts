import { tableFromArrays, tableToIPC } from "apache-arrow";
import { setPanicHook, Table } from "../../pkg/node/parquet_wasm";
import { it } from "vitest";
import { writeFileSync } from "fs";

setPanicHook();

it("should read IPC stream correctly", async (t) => {
  const table = tableFromArrays({
    column: [
      [1, 2],
      [3, 4],
    ],
  });
  console.log(table.schema);
  const ipc = tableToIPC(table, "stream");
  writeFileSync("data.arrows", ipc);

  Table.fromIPCStream(ipc);
});
