#! /usr/bin/env bash

# Build script used in Node-only CI tests

rm -rf pkg pkg_node pkg_web

######################################
# ARROW 1 (arrow-rs) the default feature
# Build node version into pkg_node
echo "Building arrow-rs node"
wasm-pack build \
  --dev \
  --out-dir pkg \
  --out-name node \
  --target nodejs

######################################
# ARROW 2 turn on the feature manually
# Build node version into pkg2_node
echo "Building arrow2 node"
wasm-pack build \
  --dev \
  --out-dir pkg2_node \
  --out-name node2 \
  --target nodejs \
  --no-default-features \
  --features arrow2 \
  --features parquet2_supported_compressions

# Copy files into pkg/
cp pkg2_node/{node2.d.ts,node2.js,node2_bg.wasm,node2_bg.wasm.d.ts} pkg/

# Update files array using JQ
jq '.files += ["node2.d.ts", "node2.js", "node2_bg.wasm", "node2_bg.wasm.d.ts"]' pkg/package.json > pkg/package.json.tmp
# Overwrite existing file
mv pkg/package.json.tmp pkg/package.json
