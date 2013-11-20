# run the benchmark and send output to OUTFILE, timings etc. to TIMINGS
PATH=$1
OUTFILE=/dev/null
TIMINGS=$2
DATABASE=$3
UNAME=$4
PWORD=$5
echo $PWORD

vsql -U $UNAME -w $PWORD -d $DATABASE -e -f ${PATH}/cstore_benchmark.sql -o $OUTFILE > $TIMINGS 2>&1