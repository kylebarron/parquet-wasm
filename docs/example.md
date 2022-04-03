## Example

```js
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import { readParquet, writeParquet } from "parquet-wasm";

// Create Arrow Table in JS
const LENGTH = 2000;
const rainAmounts = Float32Array.from({ length: LENGTH }, () =>
  Number((Math.random() * 20).toFixed(1))
);

const rainDates = Array.from(
  { length: LENGTH },
  (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i)
);

const rainfall = tableFromArrays({
  precipitation: rainAmounts,
  date: rainDates,
});

// Write Arrow Table to Parquet
const parquetBuffer = writeParquet(tableToIPC(rainfall, "stream"));

// Read Parquet buffer back to Arrow Table
const table = tableFromIPC(readParquet(parquetBuffer));
console.log(table.schema.toString());
// Schema<{ 0: precipitation: Float32, 1: date: Date64<MILLISECOND> }>
```
