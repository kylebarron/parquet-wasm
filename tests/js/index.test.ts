import * as wasm from "../../pkg/node/parquet_wasm.js";

wasm.setPanicHook();

import "./read-write.test.js";
import "./ffi.test.js";
import "./geo-metadata.test.js";
import "./schema.test.js";
import "./writer-properties.test.js";
