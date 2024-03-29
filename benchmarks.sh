#! /bin/bash

set -eux

cargo build

pushd benchmarks

# This is required from e2e.sh
if [ ! -e ./esql ]; then
    ln -s ../target/debug/esql
fi

# target scales
TARGET_SCALES=`echo tiny small medium`

#
# Any combination of tiny small medium large can be specified.
#
for scale in ${TARGET_SCALES}
do
    if [ -d ./${scale} ]; then
	rm -rf ./${scale}
    fi
    mkdir ${scale}
    ./gen-sql-for-db.sh ${scale} > ./${scale}/init-data.sql
    ./e2e.sh ${scale}
done

rm esql

popd

#
# republish data for docs
#
rm -rf docs/*
cp benchmarks/index.html docs
for scale in ${TARGET_SCALES}
do
    mkdir -p docs/${scale}/summary
    cp -r benchmarks/${scale}/summary docs/${scale}
done
