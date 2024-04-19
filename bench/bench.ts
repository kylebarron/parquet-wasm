import b from "benny";
import * as parquet from "../pkg/node";
import { readFileSync } from "fs";

const dataDir = `${__dirname}/data`;

// https://stackoverflow.com/a/43053803
const cartesian = (...a) =>
  a.reduce((a, b) => a.flatMap((d) => b.map((e) => [d, e].flat())));

const partitions = [1, 5, 20];
const compressions = ["brotli", "gzip", "none", "snappy"];

const testCases: [number, string][] = cartesian(partitions, compressions);

const createReadTests = () =>
  testCases.map(([partitions, compression, api]) => {
    const file = `${partitions}-partition-${compression}`;
    const testName = `${api} ${file}`;
    return b.add(testName, () => {
      const arr = loadFile(file);
      return () => parquet.readParquet2(arr);
    });
  });

function loadFile(name: string): Uint8Array {
  const dataPath = `${dataDir}/${name}.parquet`;
  return new Uint8Array(readFileSync(dataPath));
}

b.suite(
  "Read Parquet",

  ...createReadTests(),

  b.cycle(),
  b.configure({ minDisplayPrecision: 2 }),
  b.complete(),
  b.save({
    file: "bench",
    folder: "bench/results/",
    version: "0.3.0",
    details: true,
    format: "chart.html",
  })
);
