rm -rf pkg pkg_node pkg_web

# Build node version into pkg_node
wasm-pack build --release --out-dir pkg_node --out-name node --target nodejs

# Build web version into pkg_web
wasm-pack build --release --out-dir pkg_web --out-name web --target web

# Build standard bundler version into pkg
wasm-pack build --release --out-dir pkg --target bundler

# Copy files into pkg/
cp pkg_node/{node.d.ts,node.js,node_bg.wasm,node_bg.wasm.d.ts} pkg/
cp pkg_web/{web.d.ts,web.js,web_bg.wasm,web_bg.wasm.d.ts} pkg/

# Update files array using JQ
jq '.files += ["node.d.ts","node.js","node_bg.wasm","node_bg.wasm.d.ts", "web.d.ts","web.js","web_bg.wasm","web_bg.wasm.d.ts"]' pkg/package.json > pkg/package.json.tmp
mv pkg/package.json.tmp pkg/package.json
