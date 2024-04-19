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

# Build node version into tmp_build/node
echo "Building node"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/node \
  --target nodejs \
  $FLAGS &
[ -n "$CI" ] && wait;

# Build web version into tmp_build/esm
echo "Building esm"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm \
  --target web \
  $FLAGS &
[ -n "$CI" ] && wait;

# Build bundler version into tmp_build/bundler
echo "Building bundler"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler \
  --target bundler \
  $FLAGS &
wait


# Copy files into pkg/
mkdir -p pkg/{node,esm,bundler}

cp tmp_build/bundler/parquet* pkg/bundler/
cp tmp_build/esm/parquet* pkg/esm
cp tmp_build/node/parquet* pkg/node

cp tmp_build/bundler/{LICENSE_APACHE,LICENSE_MIT,README.md} pkg/

# Copy in combined package.json from template
# https://stackoverflow.com/a/24904276
# Note that keys from the second file will overwrite keys from the first.
jq -s '.[0] * .[1]' templates/package.json tmp_build/bundler/package.json > pkg/package.json

# Create minimal package.json in esm/ folder with type: module
echo '{"type": "module"}' > pkg/esm/package.json

# Update files array in package.json using JQ
# Set module field to bundler/arrow1.js
# Set types field to bundler/arrow1.d.ts
jq '.files = ["*"] | .module="esm/parquet_wasm.js" | .types="esm/parquet_wasm.d.ts"' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json

rm -rf tmp_build
