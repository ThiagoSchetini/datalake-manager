#!/usr/bin/env bash

transaction="FileToHive"
log="[DATA] Transaction ${transaction}:"

#-- local paths --#
test=src/test/data/FileToHiveTransaction
source1=${test}/source1.csv
source2=${test}/source2.csv
source3=${test}/source3.csv
smPath=${test}/file-to-hive.properties

#-- hdfs paths --#
transactionTestPath=/br/com/bvs/datalake/transaction/FileToHiveTransaction
watchSMPath=${transactionTestPath}/watchSM
destinyPath=${transactionTestPath}/destiny
sourcesPath=${transactionTestPath}/sources

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${destinyPath} 2>/dev/null
hdfs dfs -mkdir -p ${destinyPath}

echo "${log} drop Hive table"
hive -e "drop table if exists testdb.types;"

echo "${log} copy files"
hdfs dfs -rm -R -skipTrash ${sourcesPath} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchSMPath} 2>/dev/null
hdfs dfs -mkdir -p ${sourcesPath}
hdfs dfs -mkdir -p ${watchSMPath}
hdfs dfs -copyFromLocal ${source1} ${sourcesPath}
hdfs dfs -copyFromLocal ${source2} ${sourcesPath}
hdfs dfs -copyFromLocal ${source3} ${sourcesPath}
hdfs dfs -copyFromLocal ${smPath} ${watchSMPath}