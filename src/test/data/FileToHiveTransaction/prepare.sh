#!/usr/bin/env bash

title="FileToHiveTransaction test data:"

#-- local --#
test=src/test/data/FileToHiveTransaction
csv=${test}/file1.csv
csv2=${test}/file2.csv
csv3=${test}/file3.csv
smCSV=${test}/sm-csv.properties
smDir=${test}/sm-dir.properties

#-- hdfs --#
testHdfs=/br/com/bvs/datalake/transaction/FileToHiveTransaction
watchCSV=${testHdfs}/watchCSV
watchDir=${testHdfs}/watchDir
sourceCSV=${testHdfs}/sourceCSV/file1.csv
sourceDir=${testHdfs}/sourceDir
destiny=${testHdfs}/destiny

#-- sanitize hdfs and hive --#
hdfs dfs -rm -R -skipTrash ${destiny} 2>/dev/null
hdfs dfs -mkdir -p ${destiny}
hive -e "drop table if exists types;"

echo "[TEST] ${title} transact a single csv file"
hdfs dfs -rm -R -skipTrash ${sourceCSV} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchCSV} 2>/dev/null
hdfs dfs -mkdir -p ${sourceCSV}
hdfs dfs -mkdir -p ${watchCSV}
hdfs dfs -copyFromLocal ${csv} ${sourceCSV}
hdfs dfs -copyFromLocal ${smCSV} ${watchCSV}

echo "[TEST] ${title} transact from a dir with files"
hdfs dfs -rm -R -skipTrash ${sourceDir} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchDir} 2>/dev/null
hdfs dfs -mkdir -p ${sourceDir}
hdfs dfs -mkdir -p ${watchDir}
hdfs dfs -copyFromLocal ${csv2} ${sourceDir}
hdfs dfs -copyFromLocal ${csv3} ${sourceDir}
hdfs dfs -copyFromLocal ${smDir} ${watchDir}