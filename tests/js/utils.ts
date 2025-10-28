import { expect } from "vitest";
import { readFileSync } from "fs";
import { tableFromIPC, Table } from "apache-arrow";
import fastify, { FastifyInstance } from "fastify";
import fastifyStatic from "@fastify/static";
import { join } from "path";
const dataDir = "tests/data";

/** Test that two Arrow tables are equal */
export function testArrowTablesEqual(table1: Table, table2: Table): void {
  expect(table1.schema.metadata).toStrictEqual(table2.schema.metadata);
  expect(table1.schema.fields.length).toStrictEqual(
    table2.schema.fields.length
  );

  // Note that calling deepEquals on the schema object correctly can fail when in one schema the
  // type is Int_ with bitWidth 32 and the other has Int32.
  for (let i = 0; i < table1.schema.fields.length; i++) {
    const field1 = table1.schema.fields[i];
    const field2 = table2.schema.fields[i];
    expect(field1.name).toStrictEqual(field2.name);
    expect(field1.nullable).toStrictEqual(field2.nullable);
    // Note that calling deepEquals on the type fails! Instead you have to check the typeId
    // t.deepEquals(field1.type, field2.type);
    expect(field1.typeId).toStrictEqual(field2.typeId);
  }

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
    expect(
      vector1.toJSON(),
      `data arrays should be equal for column ${fieldName}`
    ).toStrictEqual(vector2.toJSON());
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
    root: join(__dirname, "../data"),
  });
  await server.listen({
    port: 0,
    host: "localhost",
  });
  return server as FastifyInstance;
}

export function extractFooterBytes(parquetFile: Uint8Array): Uint8Array {
  // Step 1: Obtain the last 8 bytes to get footer length and magic number.
  const TAIL_LENGTH = 8;
  const tailStartIndex = parquetFile.length - TAIL_LENGTH;
  const tailBytes = parquetFile.subarray(tailStartIndex);
  if (!tailBytes || tailBytes.length < TAIL_LENGTH) {
    throw new Error('Failed to load the Parquet footer length.');
  }

  // Step 2: Parse the footer length and magic number.
  // little-endian
  const footerLength = new DataView(tailBytes.buffer, tailBytes.byteOffset, tailBytes.byteLength).getInt32(0, true);
  const magic = new TextDecoder().decode(tailBytes.slice(4, 8));
  if (magic !== 'PAR1') {
    throw new Error('Invalid Parquet file: missing PAR1 magic number.');
  }
  
  // Step 3. Extract the footer bytes.
  const footerStartIndex = parquetFile.length - (footerLength + TAIL_LENGTH);
  // Use .slice here to ensure a fresh arrayBuffer is created,
  // so that downstream usage is not "seeing" the full parquetFile buffer.
  const footerBytes = parquetFile.slice(footerStartIndex);
  if (footerBytes.length !== footerLength + TAIL_LENGTH) {
    throw new Error('Failed to load the Parquet footer bytes.');
  }
  return footerBytes;
}