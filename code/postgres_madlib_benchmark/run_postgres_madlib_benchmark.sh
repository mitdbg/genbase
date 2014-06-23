# run the benchmark and send output to OUTFILE, timings etc. to TIMINGS
PATH=$1
OUTFILE=/dev/null
TIMINGS=$2
DATABASE=$3

psql $DATABASE -e -f ${PATH}/postgres_madlib_benchmark.sql -o $OUTFILE > $TIMINGS 2>&1