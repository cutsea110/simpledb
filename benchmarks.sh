#! /bin/bash

cargo build

pushd benchmarks

# This is required from e2e.sh
ln -s ../target/debug/esql

./e2e.sh

rm esql

popd
