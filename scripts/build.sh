#! /usr/bin/env bash
rm -rf tmp_build pkg
mkdir -p tmp_build

if [ "$ENV" == "DEV" ]; then
   BUILD="--dev"
   FLAGS="--features debug"
else
   BUILD="--release"
   FLAGS=""
fi

######################################
# ARROW 1 (arrow-rs) the default feature
# Build node version into tmp_build/node
echo "Building arrow-rs node"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/node \
  --out-name arrow1 \
  --target nodejs \
  $FLAGS

# Build web version into tmp_build/esm
echo "Building arrow-rs esm"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm \
  --out-name arrow1 \
  --target web \
  $FLAGS

# Build bundler version into tmp_build/bundler
echo "Building arrow-rs bundler"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler \
  --out-name arrow1 \
  --target bundler \
  $FLAGS

######################################
# ARROW 2 turn on the feature manually
# Build node version into tmp_build/node2
echo "Building arrow2 node"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/node2 \
  --out-name arrow2 \
  --target nodejs \
  --no-default-features \
  --features arrow2 \
  --features reader \
  --features writer \
  --features all_compressions \
  --features async \
  $FLAGS

# Build web version into tmp_build/esm2
echo "Building arrow2 esm"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm2 \
  --out-name arrow2 \
  --target web \
  --no-default-features \
  --features arrow2 \
  --features reader \
  --features writer \
  --features all_compressions \
  --features async \
  $FLAGS

# Build bundler version into tmp_build/bundler2
echo "Building arrow2 bundler"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler2 \
  --out-name arrow2 \
  --target bundler \
  --no-default-features \
  --features arrow2 \
  --features reader \
  --features writer \
  --features all_compressions \
  --features async \
  $FLAGS

# Copy files into pkg/
mkdir -p pkg/{node,esm,bundler}

cp tmp_build/{bundler,bundler2}/arrow* pkg/bundler/
cp tmp_build/{esm,esm2}/arrow* pkg/esm
cp tmp_build/{node,node2}/arrow* pkg/node

cp tmp_build/bundler/{package.json,LICENSE_APACHE,LICENSE_MIT,README.md} pkg/

# Create minimal package.json in esm/ folder with type: module
echo '{"type": "module"}' > pkg/esm/package.json

# Create an esm2/ directory without import.meta.url in the JS ESM wrapper
cp -r pkg/esm pkg/esm2
sed '/import.meta.url/ s|input|// input|' pkg/esm2/arrow1.js > pkg/esm2/arrow1_new.js
sed '/import.meta.url/ s|input|// input|' pkg/esm2/arrow2.js > pkg/esm2/arrow2_new.js
mv pkg/esm2/arrow1_new.js pkg/esm2/arrow1.js
mv pkg/esm2/arrow2_new.js pkg/esm2/arrow2.js

# Update files array in package.json using JQ
# Set module field to bundler/arrow1.js
# Set types field to bundler/arrow1.d.ts
jq '.files = ["*"] | .module="bundler/arrow1.js" | .types="bundler/arrow1.d.ts"' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json

rm -rf tmp_build
