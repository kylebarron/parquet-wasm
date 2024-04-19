import * as wasm from "../../pkg/node/parquet_wasm";

wasm.setPanicHook();

import "./read-write.test";
import "./ffi.test";
import "./geo-metadata.test";
import "./schema.test";
