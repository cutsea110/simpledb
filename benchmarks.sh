#! /bin/bash

set -eux

cargo build

pushd benchmarks

# This is required from e2e.sh
if [ ! -e ./esql ]; then
    ln -s ../target/debug/esql
fi

# default tiny
if [ -d ./tiny ]; then
    rm -rf ./tiny
fi
./gen-sql-for-db.sh tiny > ./init-tiny.sql
./e2e.sh tiny

# create small size db init sql file
## ./gen-sql-for-db.sh small > ./init-small.sql
## ./e2e.sh small

# create medium size db init sql file
## ./gen-sql-for-db.sh medium > ./init-medium.sql
## ./e2e.sh medium

# create large size db init sql file
## ./gen-sql-for-db.sh large > ./init-large.sql
## ./e2e.sh large

rm esql

popd
