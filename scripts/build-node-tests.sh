#! /usr/bin/env bash

# Build script used in Node-only CI tests

rm -rf tmp_build pkg
mkdir -p tmp_build

######################################
# ARROW 1 (arrow-rs) the default feature
# Build node version into tmp_build/node
echo "Building arrow-rs node"
wasm-pack build \
  --release \
  --out-dir tmp_build/node \
  --out-name arrow1 \
  --target nodejs

######################################
# ARROW 2 turn on the feature manually
# Build node version into tmp_build/node2
echo "Building arrow2 node"
wasm-pack build \
  --release \
  --out-dir tmp_build/node2 \
  --out-name arrow2 \
  --target nodejs \
  --no-default-features \
  --features arrow2 \
  --features reader \
  --features writer \
  --features parquet2_supported_compressions

# Copy files into pkg/
mkdir -p pkg/node
cp tmp_build/{node,node2}/arrow* pkg/node
cp tmp_build/node/{package.json,LICENSE_APACHE,LICENSE_MIT,README.md} pkg/

# Update files array in package.json using JQ
# Set main field to node/arrow1.js
# Set types field to node/arrow1.d.ts
jq '.files = ["*"] | .main="node/arrow1.js" | .types="node/arrow1.d.ts"' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json
