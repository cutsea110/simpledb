#! /bin/bash

set -eux

cargo build

pushd benchmarks

# This is required from e2e.sh
if [ ! -e ./esql ]; then
    ln -s ../target/debug/esql
fi

#
# Any combination of tiny small medium large can be specified.
#
for scale in tiny small
do
    if [ -d ./${scale} ]; then
	rm -rf ./${scale}
    fi
    ./gen-sql-for-db.sh ${scale} > ./init-${scale}.sql
    ./e2e.sh ${scale}
done

rm esql

popd
