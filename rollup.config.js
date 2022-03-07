import typescript from "@rollup/plugin-typescript";
import rust from "@wasm-tool/rollup-plugin-rust";

export default [
  {
    input: "index.ts",
    output: {
      file: "dist/parquet.js",
      format: "umd",
      sourcemap: true,
      name: "parquet",
    },
    plugins: [
      rust({
        nodejs: false,
        // Since I already have a global installation of wasm-pack
        // https://github.com/wasm-tool/rollup-plugin-rust#build-options
        wasmPackPath: "~/.cargo/bin/wasm-pack",
      }),
      typescript(),
    ],
  },
  {
    input: "index.ts",
    output: {
      file: "dist/parquet-node.js",
      format: "umd",
      sourcemap: true,
      name: "parquet",
    },
    plugins: [
      rust({
        nodejs: true,
        inlineWasm: true,
        // Since I already have a global installation of wasm-pack
        // https://github.com/wasm-tool/rollup-plugin-rust#build-options
        wasmPackPath: "~/.cargo/bin/wasm-pack",
      }),
      typescript(),
    ],
  },
];
