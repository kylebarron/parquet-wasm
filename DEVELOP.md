# Development

- Install [wasm-pack](https://rustwasm.github.io/wasm-pack/)
- Compile: `wasm-pack build`, or change targets, e.g. `wasm-pack build --target nodejs`
- Publish `wasm-pack publish`.

### MacOS

Some steps may need a specific configuration if run on MacOS. Specifically, the default `clang` shipped with Macs (as of March 2022) doesn't have WebAssembly compilation supported out of the box. To build ZSTD, you may need to install a later version via Homebrew and update your paths to find the correct executables.

```
brew install llvm
export PATH="/usr/local/opt/llvm/bin/:$PATH"
export CC=/usr/local/opt/llvm/bin/clang
export AR=/usr/local/opt/llvm/bin/llvm-ar
```

See [this description](https://github.com/kylebarron/parquet-wasm/pull/2#issue-1159174043) and its references for more info.

## Publishing

`wasm-pack` supports [three different targets](https://rustwasm.github.io/docs/wasm-pack/commands/build.html#target):

- `bundler` (used with bundlers like Webpack)
- `nodejs` (used with Node, supports `require`)
- `web` (used as an ES module directly from the web)

There are good reasons to distribute as any of these... so why not distribute as all three? `wasm-pack` doesn't support this directly but the build script in `scripts/build.sh` calls `wasm-pack` three times and merges the outputs. This means that bundler users can use the default, Node users can use `parquet-wasm/node` and ES Modules users can use `parquet-wasm/web` in their imports.

To publish:

```
bash ./scripts/build.sh
wasm-pack publish
```
