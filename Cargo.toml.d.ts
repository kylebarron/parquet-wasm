export * from "./target/wasm-pack/parquet-wasm/index";

type Exports = typeof import("./target/wasm-pack/parquet-wasm/index");
declare const init: () => Promise<Exports>;
export default init;
