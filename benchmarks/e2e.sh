#!/bin/sh

# This script requires awk and jq, and a link for esql binary build image.

set -xu

#
# DBSIZE: tiny, small, medium, large
#
DBSIZE=${1:-tiny}

LOG_DIR=./${DBSIZE}/logs
mkdir -p ${LOG_DIR}
JSON_DIR=./${DBSIZE}/json
mkdir -p ${JSON_DIR}
SUMMARY_DIR=./${DBSIZE}/summary
mkdir -p ${SUMMARY_DIR}
# It is assumed that a link to esql exists.
# See benchmarks.sh.
ESQL=./esql

INIT_SQL=`cat init-${DBSIZE}.sql`
QUERY_SQL=`cat query.sql` # This query must be independent of DBSIZE.

for bm in naive naivebis fifo lru clock
do
    for qp in basic heuristic
    do
	# clear db
	rm -rf data
	# init db
	RUST_LOG=trace \
		${ESQL} -d demo \
		--buffer-manager ${bm} \
		--query-planner ${qp} \
		2> ${LOG_DIR}/${bm}_${qp}_init.log <<EOM
yes

${INIT_SQL}

:q
EOM
	# query db
	# couldn't use view, because basic query planner can't correctly handle view table.
	RUST_LOG=trace \
		${ESQL} -d demo \
		--buffer-manager ${bm} \
		--query-planner ${qp} \
		2> ${LOG_DIR}/${bm}_${qp}_query.log <<EOM

${QUERY_SQL}

:q
EOM
	# convert to json
	awk -F'[/ ()]' -f log2json.awk -- \
	    ${LOG_DIR}/${bm}_${qp}_query.log | \
	    \jq -c > ${JSON_DIR}/${bm}_${qp}.json
    done
done

# merge all data
cat ${JSON_DIR}/*.json | \
    \jq -s '[.[] |
             .config."buffer-manager"           as $bm    |
             .config."query-planner"            as $qp    |
             (.config."block-size"|tostring)    as $blksz |
             (.config."buffer-size"|tostring)   as $bfsz  |
             ($bm+"-"+$qp+"-"+$blksz+"-"+$bfsz) as $nm    |
             { "name": $nm
             , "read": (.records | map(."file-manager".read))
             , "written": (.records | map(."file-manager".written))
             , "pinned": (.records | map(."buffer-manager".pinned))
             , "unpinned": (.records | map(."buffer-manager".unpinned))
             , "hit": (.records | map(."buffer-manager".cache.hit))
             , "assigned": (.records | map(."buffer-manager".cache.assigned))
             , "ratio": (.records | map(."buffer-manager".cache.ratio))
             , "elapsed" : (.records | map(."elapsed-time"))
             }
            ]' > ${SUMMARY_DIR}/data.json

# names of database construction
cat ${SUMMARY_DIR}/data.json | \
    \jq -c '[.[] | .name]' > ${SUMMARY_DIR}/name.json

# the other measures
for k in read written pinned unpinned hit assigned ratio elapsed
do
    cat ${SUMMARY_DIR}/data.json | \
	\jq -c "[.[] |
                 .${k}] | transpose |
                 to_entries |
                 map(. |= [\"Q\"+(.key+1|tostring)]+.value)" > ${SUMMARY_DIR}/${k}.json
done
