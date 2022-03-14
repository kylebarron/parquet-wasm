#! /usr/bin/env bash
rm -rf pkg pkg_node pkg_web

######################################
# ARROW 1 (arrow-rs) the default feature
# Build node version into pkg_node
echo "Building arrow-rs node"
wasm-pack build \
  --release \
  --out-dir pkg_node \
  --out-name node \
  --target nodejs

# Build web version into pkg_web
echo "Building arrow-rs web"
wasm-pack build \
  --release \
  --out-dir pkg_web \
  --out-name web \
  --target web

# Build standard bundler version into pkg
echo "Building arrow-rs bundler"
wasm-pack build \
  --release \
  --out-dir pkg \
  --target bundler

######################################
# ARROW 2 turn on the feature manually
# Build node version into pkg2_node
echo "Building arrow2 node"
wasm-pack build \
  --release \
  --out-dir pkg2_node \
  --out-name node2 \
  --target nodejs \
  --no-default-features \
  --features arrow2 \
  --features parquet_compression

# Build web version into pkg2_web
echo "Building arrow2 web"
wasm-pack build \
  --release \
  --out-dir pkg2_web \
  --out-name web2 \
  --target web \
  --no-default-features \
  --features arrow2 \
  --features parquet_compression

# Build standard bundler version into pkg2
echo "Building arrow2 bundler"
wasm-pack build \
  --release \
  --out-dir pkg2 \
  --out-name parquet_wasm2 \
  --target bundler \
  --no-default-features \
  --features arrow2 \
  --features parquet_compression

# Copy files into pkg/
cp pkg_node/{node.d.ts,node.js,node_bg.wasm,node_bg.wasm.d.ts} pkg/
cp pkg_web/{web.d.ts,web.js,web_bg.wasm,web_bg.wasm.d.ts} pkg/

cp pkg2_node/{node2.d.ts,node2.js,node2_bg.wasm,node2_bg.wasm.d.ts} pkg/
cp pkg2_web/{web2.d.ts,web2.js,web2_bg.wasm,web2_bg.wasm.d.ts} pkg/
cp pkg2/{parquet_wasm2.d.ts,parquet_wasm2.js,parquet_wasm2_bg.wasm,parquet_wasm2_bg.wasm.d.ts} pkg/

# Update files array using JQ
jq '.files += [
  "node.d.ts", "node.js", "node_bg.wasm", "node_bg.wasm.d.ts",
  "web.d.ts", "web.js", "web_bg.wasm", "web_bg.wasm.d.ts",

  "node2.d.ts", "node2.js", "node2_bg.wasm", "node2_bg.wasm.d.ts",
  "web2.d.ts", "web2.js", "web2_bg.wasm", "web2_bg.wasm.d.ts",
  "parquet_wasm2.d.ts", "parquet_wasm2.js", "parquet_wasm2_bg.wasm", "parquet_wasm2_bg.wasm.d.ts"
  ]' pkg/package.json > pkg/package.json.tmp
# Overwrite existing file
mv pkg/package.json.tmp pkg/package.json
