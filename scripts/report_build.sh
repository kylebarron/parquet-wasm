rm -rf report_pkg
mkdir -p report_pkg

echo "Building arrow-rs slim"
wasm-pack build \
  --release \
  --no-pack \
  --out-dir report_pkg/slim \
  --out-name parquet_wasm \
  --target web
echo "Building arrow-rs sync"
wasm-pack build \
  --release \
  --no-pack \
  --out-dir report_pkg/sync \
  --out-name parquet_wasm \
  --target web \
  --features={reader,writer,all_compressions} &

echo "Building arrow-rs async_full"
wasm-pack build \
  --release \
  --no-pack \
  --out-dir report_pkg/async_full \
  --out-name parquet_wasm \
  --target web \
  --features={reader,writer,all_compressions,async} &

wait;