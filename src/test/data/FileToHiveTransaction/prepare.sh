#!/usr/bin/env bash

title="FileToHiveTransaction:"

#-- local paths --#
test=src/test/data/FileToHiveTransaction
csv=${test}/file1.csv
csv2=${test}/file2.csv
csv3=${test}/file3.csv
smSingleCSV=${test}/sm-single-csv.properties
smMultiCSV=${test}/sm-multi-csv.properties

#-- hdfs paths --#
testHdfs=/br/com/bvs/datalake/transaction/FileToHiveTransaction
watchSingleCSVSM=${testHdfs}/watchSingleCSVSM
watchMultiCSVSM=${testHdfs}/watchMultiCSVSM
sourceSingleCSV=${testHdfs}/sourceSingleCSV
sourceMultiCSV=${testHdfs}/sourceMultiCSV
destiny=${testHdfs}/destiny

#-- sanitize hdfs and hive --#
echo "[DATA] ${title} prepare destiny"
hdfs dfs -rm -R -skipTrash ${destiny} 2>/dev/null
hdfs dfs -mkdir -p ${destiny}
hive -e "drop table if exists testdb.types;"

echo "[DATA] ${title} prepare single csv"
hdfs dfs -rm -R -skipTrash ${sourceSingleCSV} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchSingleCSVSM} 2>/dev/null
hdfs dfs -mkdir -p ${sourceSingleCSV}
hdfs dfs -mkdir -p ${watchSingleCSVSM}
hdfs dfs -copyFromLocal ${csv} ${sourceSingleCSV}
hdfs dfs -copyFromLocal ${smSingleCSV} ${watchSingleCSVSM}

echo "[DATA] ${title} prepare multi csv"
hdfs dfs -rm -R -skipTrash ${sourceMultiCSV} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchMultiCSVSM} 2>/dev/null
hdfs dfs -mkdir -p ${sourceMultiCSV}
hdfs dfs -mkdir -p ${watchMultiCSVSM}
hdfs dfs -copyFromLocal ${csv2} ${sourceMultiCSV}
hdfs dfs -copyFromLocal ${csv3} ${sourceMultiCSV}
hdfs dfs -copyFromLocal ${smMultiCSV} ${watchMultiCSVSM}