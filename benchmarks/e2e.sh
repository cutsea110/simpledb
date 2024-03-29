#!/bin/sh

# This script requires awk and jq, and a link for esql binary build image.

set -eux

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

INIT_SQL=`cat ${DBSIZE}/init-data.sql`
QUERY_SQL=`cat query.sql` # This query must be independent of DBSIZE.

#
# DATABASE Construction Parameters
#
case "${DBSIZE}" in
    "tiny")
	BUFFER_MANAGERS_STR='"naive","naiveup","naivebis","naivebisup","fifo","fifots","fifoup","lru","lruts","lruup","clock","clockup"'
	QUERY_PLANNERS_STR='"basic","heuristic"'
	BLOCK_SIZES_STR='400,800,1200,2000,4000'
	BUFFER_SIZES_STR='8,16,32,64,128'
	;;
    "small")
	BUFFER_MANAGERS_STR='"naive","naiveup","naivebis","naivebisup","fifo","fifots","fifoup","lru","lruts","lruup","clock","clockup"'
	QUERY_PLANNERS_STR='"basic","heuristic"'
	BLOCK_SIZES_STR='400,800,1200,2000,4000'
	BUFFER_SIZES_STR='8,16,32,64,128'
	;;
    "medium")
	BUFFER_MANAGERS_STR='"naivebis","fifo","fifots","fifoup","lru","lruts","lruup","clock","clockup"'
	QUERY_PLANNERS_STR='"heuristic"'
	BLOCK_SIZES_STR='400,1000,2000,4000'
	BUFFER_SIZES_STR='8,16,32,64,128,1024,2048'
	;;
    "large")
	BUFFER_MANAGERS_STR='"naive","naivebis","fifo","lru","clock"'
	QUERY_PLANNERS_STR='"heuristic"'
	BLOCK_SIZES_STR='400,800,1200,2000,4000'
	BUFFER_SIZES_STR='8,16,32,64,128'
	;;
esac

# to array
BUFFER_MANAGERS=`echo ${BUFFER_MANAGERS_STR} | sed 's/"//g' | tr "," "\n"`
QUERY_PLANNERS=`echo ${QUERY_PLANNERS_STR} | sed 's/"//g' | tr "," "\n"`
BLOCK_SIZES=`echo ${BLOCK_SIZES_STR} | sed 's/"//g' | tr "," "\n"`
BUFFER_SIZES=`echo ${BUFFER_SIZES_STR} | sed 's/"//g' | tr "," "\n"`

for blksz in ${BLOCK_SIZES}
do
    # clear db
    rm -rf data

    # init db
    RUST_LOG=trace \
	    ${ESQL} -d demo \
	    --block-size ${blksz} \
	    --buffer-size 8 \
	    2> ${LOG_DIR}/${blksz}_init.log <<EOM
yes

${INIT_SQL}

:q
EOM
		
    for qp in ${QUERY_PLANNERS}
    do
	for bm in ${BUFFER_MANAGERS}
	do
	    for bfsz in ${BUFFER_SIZES}
	    do
		CONSTRUCT=${bm}_${qp}_${blksz}x${bfsz}
		# query db
		# couldn't use view, because basic query planner can't correctly handle view table.
		RUST_LOG=trace \
			${ESQL} -d demo \
			--buffer-manager ${bm} \
			--query-planner ${qp} \
			--block-size ${blksz} \
			--buffer-size ${bfsz} \
			2> ${LOG_DIR}/${CONSTRUCT}_query.log <<EOM

${QUERY_SQL}

:q
EOM
		# convert to json from log
		./log2json ${LOG_DIR}/${CONSTRUCT}_query.log | \
		    \jq -c > ${JSON_DIR}/${CONSTRUCT}.json
	    done
	done
    done
done

for qp in ${QUERY_PLANNERS}
do
    for bm in ${BUFFER_MANAGERS}
    do
	# merge over the same buffer-manager and query-planner
	cat `ls --color=never -rt ${JSON_DIR}/${bm}_${qp}_*.json` | \
	    \jq -s '[.[] |
                     .config."query-planner"          as $qp    |
                     .config."buffer-manager"         as $bm    |
                     (.config."block-size"|tostring)  as $blksz |
                     (.config."buffer-size"|tostring) as $bfsz  |
                     ($blksz+"x"+$bfsz)               as $nm    |
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
                    ]' > ${SUMMARY_DIR}/${bm}_${qp}_data.json
	# generate metrics
	cat ${SUMMARY_DIR}/${bm}_${qp}_data.json | \
            \jq -c '{ "rw": ([["construct", "read", "write", "elapsed (sec)"]] +
                             [.[] |
                              (.read|length)    as $rN  |
                              (.written|length) as $wN  |
                              (.elapsed|add)    as $ttl |
                              [.name, .read[$rN-1], .written[$wN-1], $ttl]
                             ]),
                      "cache": ([["construct", "assigned", "cache hit", "ratio (%)"]] +
                                [.[] |
                                 (.assigned|length) as $aN |
                                 (.hit|length) as $hN |
                                 (.ratio|length) as $rN |
                                 [.name, .assigned[$aN-1], .hit[$hN-1], .ratio[$rN-1]]
                                ])
                    }' > ${SUMMARY_DIR}/${bm}_${qp}_metrics.json
    done
done

for blksz in ${BLOCK_SIZES}
do
    for bfsz in ${BUFFER_SIZES}
    do
	# merge over the same block-size and buffer-size
	cat `ls --color=never -rt ${JSON_DIR}/*_${blksz}x${bfsz}.json` | \
	    \jq -s '[.[] |
                     .config."query-planner"          as $qp    |
                     .config."buffer-manager"         as $bm    |
                     (.config."block-size"|tostring)  as $blksz |
                     (.config."buffer-size"|tostring) as $bfsz  |
                     ($bm+"-"+$qp)                    as $nm    |
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
                    ]' > ${SUMMARY_DIR}/${blksz}x${bfsz}_data.json
	# generate metrics
	cat ${SUMMARY_DIR}/${blksz}x${bfsz}_data.json | \
            \jq -c '{ "rw": ([["construct", "read", "write", "elapsed (sec)"]] +
                             [.[] |
                              (.read|length)    as $rN  |
                              (.written|length) as $wN  |
                              (.elapsed|add)    as $ttl |
                              [.name, .read[$rN-1], .written[$wN-1], $ttl]
                             ]),
                      "cache": ([["construct", "assigned", "cache hit", "ratio (%)"]] +
                                [.[] |
                                 (.assigned|length) as $aN |
                                 (.hit|length) as $hN |
                                 (.ratio|length) as $rN |
                                 [.name, .assigned[$aN-1], .hit[$hN-1], .ratio[$rN-1]]
                                ])
                    }' > ${SUMMARY_DIR}/${blksz}x${bfsz}_metrics.json
    done
done

#
# Benchmarking Parameters
#
cat > ${SUMMARY_DIR}/parameters.json <<EOM
{ "buffer_managers": [${BUFFER_MANAGERS_STR}]
, "query_planners":  [${QUERY_PLANNERS_STR}]
, "block_sizes":     [${BLOCK_SIZES_STR}]
, "buffer_sizes":    [${BUFFER_SIZES_STR}]
}
EOM
