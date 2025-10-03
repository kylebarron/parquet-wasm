import * as wasm from "../../pkg/node/parquet_wasm";

wasm.setPanicHook();

import "./ffi.test";
import "./geo-metadata.test";
import "./ipc.test";
import "./read-write.test";
import "./schema.test";
