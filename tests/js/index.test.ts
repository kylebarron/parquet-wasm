import { webcrypto } from "node:crypto";
// @ts-expect-error
globalThis.crypto = webcrypto;

import "./arrow1.test";
import "./arrow1-ffi.test";
import "./arrow2.test";
import "./arrow2-geo-metadata.test";
import "./arrow2-ffi.test";
