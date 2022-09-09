import * as test from "tape";
import * as wasm from "../../pkg/node/arrow2";
import { readFileSync } from "fs";
import { RecordBatch, Table, tableFromIPC, tableToIPC } from "apache-arrow";
import { testArrowTablesEqual, readExpectedArrowData } from "./utils";
import {PARQUET_URL} from './constants';
import {server} from './mocks/server'

test("hello world", async (t) => {
  server.listen();

  const resp = await fetch(PARQUET_URL);
  // console.log('resp' ,resp)

  console.log('url', PARQUET_URL)
  const test = await wasm.readMetadataAsync(PARQUET_URL, 4000);
  console.log('test', test)

  // server.close()
  t.end()
});
