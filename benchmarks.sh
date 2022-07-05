#! /bin/bash

cargo build

pushd benchmarks

# This is required from e2e.sh
ln -s ../target/debug/esql

# default tiny
./e2e.sh tiny

# create small size db init sql file
./gen-sql-for-db.sh small > ./init-small.sql
./e2e.sh small

rm esql

popd
