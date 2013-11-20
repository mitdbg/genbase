echo "--------Subsetting data--------"
start_time=$(date +%s)
echo "Start time: ${start_time}" >> $result_file
$hive_path -e "${hive_setup}drop table if exists geo_subset;drop table if exists expr_avgs;set hive.index.compact.file=/tmp/patientid_geo_index;create table geo_subset as select /*+ MAPJOIN(patients)*/ geo.* from patients join geo on (geo.patientid=patients.patientid) where patients.disease = 5;"

echo "--------Normalize data--------"
$hive_path -e "${hive_setup}set hive.index.compact.file=/tmp/geneid_geo_index;create table expr_avgs as select geneid, avg(expr_value) as avg_expr_value from geo_subset group by geneid; select /*+ MAPJOIN(y)*/ g.patientid, g.geneid, g.expr_value-y.avg_expr_value from geo_subset g join expr_avgs y on (g.geneid=y.geneid);" > "${processed_data_path}GEO-$1-$2-covar.txt" 
end_time=$(date +%s)
echo "Time for data management 1: $(($end_time - $start_time))" >> $result_file

echo "--------Format data for mahout--------"
python format_data_for_mahout.py "${processed_data_path}GEO-$1-$2-covar.txt" "${processed_data_path}"

echo "--------Create seq file--------"
javac -cp $class_path csv2seq.java
extra=1
num_rows=$(awk 'BEGIN {max = 0} {if ($1>max) max=$1} END {print max}' ${processed_data_path}GEO-$1-$2-covar.txt)
num_cols=$(awk 'BEGIN {max = 0} {if ($2>max) max=$2} END {print max}' ${processed_data_path}GEO-$1-$2-covar.txt)

java -cp $class_path csv2seq ${processed_data_path}GEO-$1-$2-covar.txt_mahout $(($num_rows + $extra)) ${processed_data_path}GEO-$1-$2-covar.txt_mahout.seq

echo "--------Move file to HDFS for mahout--------"
if $hadoop_path fs -test -e ${hdfs_data_path}GEO-$1-$2-covar.txt_mahout.seq; then
    $hadoop_path dfs -rm ${hdfs_data_path}GEO-$1-$2-covar.txt_mahout.seq
fi
$hadoop_path dfs -copyFromLocal ${processed_data_path}GEO-$1-$2-covar.txt_mahout.seq ${hdfs_data_path}GEO-$1-$2-covar.txt_mahout.seq

start_time=$(date +%s)
echo "--------Transpose matrix--------"
if $hadoop_path fs -test -e ${hdfs_data_path}transpose; then
    $hadoop_path dfs -rmr ${hdfs_data_path}transpose
fi

$mahout_path transpose --numRows $num_rows --numCols $num_cols --input ${hdfs_data_path}GEO-$1-$2-covar.txt_mahout.seq
$hadoop_path dfs -mv ${hdfs_data_path}transpose-* ${hdfs_data_path}transpose

echo "--------Matrix multiply--------"
if $hadoop_path fs -test -e ${hdfs_data_path}covar; then
    $hadoop_path dfs -rmr ${hdfs_data_path}covar
fi
$mahout_path matrixmult --numRowsA $num_rows --numColsA $num_cols --numRowsB $num_rows --numColsB $num_cols --inputPathA ${hdfs_data_path}transpose --inputPathB ${hdfs_data_path}transpose --tempDir ${hdfs_data_path}covar-tmp
$hadoop_path dfs -mv ${hdfs_data_path}productWith-* ${hdfs_data_path}covar
end_time=$(date +%s)
### Covariance needs a last step of dividing each element of the matrix; 
### however, since we only care about the top percent of pairs, we can skip this step 
echo "Time for mahout: $(($end_time - $start_time))" >> $result_file

echo "--------Convert mahout result to csv--------"
javac -cp $class_path seq2csv.java
rm ${processed_data_path}covar-part-00000
$hadoop_path dfs -copyToLocal ${hdfs_data_path}covar/part-00000 ${processed_data_path}covar-part-00000
java -cp $class_path seq2csv ${processed_data_path}covar-part-00000 ${processed_data_path}GEO-$1-$2-covar-results.txt

start_time=$(date +%s)
echo "--------Post covariance data management--------"
$hive_path -e "drop table if exists geo_covar"
$hive_path -e "drop table if exists geo_covar_tmp"

$hive_path -e "create table geo_covar(geneid1 INT, geneid2 INT, covar FLOAT) clustered by (geneid1) sorted by (geneid1) into 100 buckets row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;"

$hive_path -e "load data local inpath '${processed_data_path}GEO-$1-$2-covar-results.txt' overwrite into table geo_covar;"

max_covar=$($hive_path -e 'select max(covar)*0.01 from geo_covar;')

$hive_path -e "${hive_setup}create table geo_covar_tmp as select /*+ MAPJOIN(genes)*/ * from genes g1 join geo_covar on (g1.geneid=geo_covar.geneid1) where geo_covar.covar > ${max_covar};"
$hive_path -e "${hive_setup}select /*+ MAPJOIN(genes)*/ * from genes g1 join geo_covar_tmp on (g1.geneid=geo_covar_tmp.geneid2)" > ${processed_data_path}covar-final.txt;
echo "--------Done--------"
end_time=$(date +%s)
echo "Time for data management 2: $(($end_time - $start_time))" >> $result_file
