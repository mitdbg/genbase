start_time=$(date +%s)
echo "--------Subsetting data--------"
$hive_path -e "${hive_setup}select /*+ MAPJOIN(genes)*/ geo.* from genes join geo on (geo.geneid=genes.geneid) where genes.func < 500" > "${processed_data_path}GEO-$1-$2-svd.txt"
end_time=$(date +%s)
echo "Time in data management: $(( $end_time - $start_time ))" >> $result_file
echo "--------Format data for Mahout--------"
python format_data_for_mahout.py "${processed_data_path}GEO-$1-$2-svd.txt" "${processed_data_path}"

# create a sequence file from the CSV
echo "--------Create seq file--------"
javac -cp $class_path csv2seq.java

extra=1
num_rows=$(awk 'BEGIN {max = 0} {if ($1>max) max=$1} END {print max}' ${processed_data_path}GEO-$1-$2-svd.txt)
num_cols=$(awk 'BEGIN {max = 0} {if ($2>max) max=$2} END {print max}' ${processed_data_path}GEO-$1-$2-svd.txt)

java -cp $class_path csv2seq ${processed_data_path}GEO-$1-$2-svd.txt_mahout $(($num_cols + $extra)) ${processed_data_path}GEO-$1-$2-svd.txt_mahout.seq

echo "--------Move file to HDFS for mahout--------"
if $hadoop_path fs -test -e ${hdfs_data_path}GEO-$1-$2-svd.txt_mahout.seq; then
    $hadoop_path dfs -rm ${hdfs_data_path}GEO-$1-$2-svd.txt_mahout.seq
fi
$hadoop_path dfs -put ${processed_data_path}GEO-$1-$2-svd.txt_mahout.seq ${hdfs_data_path}GEO-$1-$2-svd.txt_mahout.seq

start_time=$(date +%s)
echo "--------Call SVD--------"
$mahout_path svd -i ${hdfs_data_path}GEO-$1-$2-svd.txt_mahout.seq -o svdout.out -nr $(($num_rows + $extra)) -nc $(($num_cols + $extra)) -r 50 -sym false --cleansvd true
end_time=$(date +%s)
echo "Time in analytics $(( $end_time - $start_time ))" >> $result_file
