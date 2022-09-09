import { rest } from "msw";

import { readFileSync } from "fs";
import { RecordBatch, Table, tableFromIPC } from "apache-arrow";
import {PARQUET_URL} from '../constants';

// Path from repo root
const dataDir = "tests/data";
const testFile = "2-partition-brotli.parquet";
const dataPath = `${dataDir}/${testFile}`;
const arr = new Uint8Array(readFileSync(dataPath));


export const handlers = [
  rest.get("https://helloworld.com", (req, res, ctx) => {
    console.log('hello world.com!');

    return res(
      // Respond with a 200 status code
      ctx.status(200),
    )

  }),
  rest.get(PARQUET_URL, (req, res, ctx) => {
    // Request for a parquet file
    console.log("req", req);
    console.log("res", res);
    console.log("ctx", ctx);

    console.log(req.headers);
    // ctx.body()

    return res(
      // Respond with a 200 status code
      ctx.status(200),
    )

  }),
];
