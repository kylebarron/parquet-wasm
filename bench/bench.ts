import b from "benny";
import * as arrow1 from "../pkg/node/arrow1";
import * as arrow2 from "../pkg/node/arrow2";
import { readFileSync } from "fs";

const dataDir = `${__dirname}/data`;

// https://stackoverflow.com/a/43053803
const cartesian = (...a) =>
  a.reduce((a, b) => a.flatMap((d) => b.map((e) => [d, e].flat())));

const apis = ["arrow1", "arrow2"];
const partitions = [1, 5, 20];
const compressions = ["brotli", "gzip", "none", "snappy"];

const testCases: [number, string, "arrow1" | "arrow2"][] = cartesian(
  partitions,
  compressions,
  apis
);

const createReadTests = () =>
  testCases.map(([partitions, compression, api]) => {
    const file = `${partitions}-partition-${compression}`;
    const testName = `${api} ${file}`;
    return b.add(testName, () => {
      const arr = loadFile(file);
      if (api === "arrow1") {
        return () => arrow1.readParquet(arr);
      }

      return () => arrow2.readParquet2(arr);
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
