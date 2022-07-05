#! /bin/bash

cargo build

pushd benchmarks

# This is required from e2e.sh
ln -s ../target/debug/esql

# default tiny
./e2e.sh tiny

rm esql

popd
