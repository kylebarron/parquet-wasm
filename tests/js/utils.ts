import { Test } from "tape";
import { readFileSync } from "fs";
import { tableFromIPC, Table } from "apache-arrow";
import fastify, { FastifyInstance } from 'fastify';
import fastifyStatic from "@fastify/static";
import path = require("path");

const dataDir = "tests/data";

/** Test that two Arrow tables are equal */
export function testArrowTablesEqual(
  t: Test,
  table1: Table,
  table2: Table
): void {
  // It seems fine to just call deepEquals directly on the schema object
  t.deepEquals(table1.schema, table2.schema, "schemas are equal");

  // However deepEquals on the table itself can give false negatives because Arrow tables can have
  // different underlying memory for the same data representation, i.e. if one table has one record
  // batch and the other has two
  const fieldNames = table1.schema.fields.map((f) => f.name);
  for (const fieldName of fieldNames) {
    const vector1 = table1.getChild(fieldName);
    const vector2 = table2.getChild(fieldName);

    // Ideally we'd be checking vector1.toArray() against vector2.toArray(), but there's apparently
    //   a bug in arrow JS, so for now we use .toJSON() to check for comparison :shrug:
    //   not ok 23 RangeError: offset is out of bounds
    // ---
    //   operator: error
    //   stack: |-
    //     RangeError: offset is out of bounds
    //         at Uint8Array.set (<anonymous>)
    //         at data.reduce.array (/Users/kyle/github/rust/parquet-wasm/node_modules/apache-arrow/src/vector.ts:256:36)
    //         at Array.reduce (<anonymous>)
    //         at Vector.toArray (/Users/kyle/github/rust/parquet-wasm/node_modules/apache-arrow/src/vector.ts:255:42)
    //         at testArrowTablesEqual (/Users/kyle/github/rust/parquet-wasm/tests/js/utils.ts:25:15)
    //         at /Users/kyle/github/rust/parquet-wasm/tests/js/arrow1.ts:46:25
    //         at step (/Users/kyle/github/rust/parquet-wasm/tests/js/arrow1.ts:33:23)
    //         at Object.next (/Users/kyle/github/rust/parquet-wasm/tests/js/arrow1.ts:14:53)
    //         at /Users/kyle/github/rust/parquet-wasm/tests/js/arrow1.ts:8:71
    //         at new Promise (<anonymous>)
    // ...
    t.deepEquals(
      vector1.toJSON(),
      vector2.toJSON(),
      `data arrays should be equal for column ${fieldName}`
    );
  }
}

/** Load expected arrow data written from Python in Arrow IPC File format */
export function readExpectedArrowData(): Table {
  const expectedArrowPath = `${dataDir}/data.arrow`;
  const buffer = readFileSync(expectedArrowPath);
  return tableFromIPC(buffer);
}

export async function temporaryServer() {
  const server = fastify().register(fastifyStatic, {
    root: path.join(__dirname, '../data')
  });
  await server.listen({
    port: 0,
    host: 'localhost'
  });
  return server as FastifyInstance;

}