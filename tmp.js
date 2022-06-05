console.log('hi')
var wasm = require('./tmp/arrow1')

var path = 'tests/data/2-partition-snappy.parquet'
var {readFileSync} = require('fs')
var buffer = readFileSync(path)
var arr = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
wasm.readParquetMetadata(arr)
wasm.readParquet(arr)

// wasm.Encoding

import {WriterPropertiesBuilder, Compression, writeParquet} from './pkg';
const writerProperties = new WriterPropertiesBuilder().setCompression(Compression.SNAPPY).build();
writeParquet(new Uint8Array(), writerProperties);
