#!/bin/bash  

# run as follows: 
# benchmark_final.sh geomatrix_rows geomatrix_cols covar|svd
 
result_file=run-$1-$2-$3.txt
hive_path=
orig_data_path=
processed_data_path=
class_path=hadoop-core-1.2.1.jar:mahout-core-0.7.jar:mahout-math-0.7.jar:commons-logging-1.1.1.jar:commons-configuration-1.6.jar:commons-lang-2.4.jar:guava-r09.jar:.
hadoop_path=
mahout_path=
hdfs_data_path=/hdfs_data/$1-$2/
hive_setup="${hive_setup}set hive.mapred.local.mem=4000;set hive.smalltable.filesize = 4000000;set hive.auto.convert.join=true;"

# load data
source load_final.sh

if [ $3 == "covar" ]
  then
    source covar.sh
fi

if [ $3 == "svd" ]
  then
    source svd.sh
fi
