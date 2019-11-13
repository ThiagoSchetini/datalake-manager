#!/usr/bin/env bash

testPath=src/test/data/FileToHiveTransaction
hdfsPath=/br/com/bvs/datalake/transaction/FileToHiveTransaction

watch1=${hdfsPath}/watch1
watch2=${hdfsPath}/watch2
sm1=${testPath}/sm1.properties
sm2=${testPath}/sm2.properties

sourcePath1=${hdfsPath}/source1/file1.csv
sourcePath2=${hdfsPath}/source2
testSource1=${testPath}/file1.csv
testSource2=${testPath}/file2.csv
testSource3=${testPath}/file3.csv

destinationPath=${hdfsPath}/destiny

echo "[INFO] FileToHiveTransaction: prepare sm's to watched folders"
hdfs dfs -rm -R -skipTrash ${watch1} 2>/dev/null
hdfs dfs -mkdir -p ${watch1}
hdfs dfs -copyFromLocal ${sm1} ${watch1}

hdfs dfs -rm -R -skipTrash ${watch2} 2>/dev/null
hdfs dfs -mkdir -p ${watch2}
hdfs dfs -copyFromLocal ${sm2} ${watch2}

echo "[INFO] FileToHiveTransaction: prepare destiny"
hdfs dfs -rm -R -skipTrash ${destinationPath} 2>/dev/null
hdfs dfs -mkdir -p ${destinationPath}

echo "[INFO] FileToHiveTransaction: prepare single file source"
hdfs dfs -rm -R -skipTrash ${sourcePath1} 2>/dev/null
hdfs dfs -mkdir -p ${sourcePath1}
hdfs dfs -copyFromLocal ${testSource1} ${sourcePath1}

echo "[INFO] FileToHiveTransaction: prepare directory with sources"
hdfs dfs -rm -R -skipTrash ${sourcePath2} 2>/dev/null
hdfs dfs -mkdir -p ${sourcePath2}
hdfs dfs -copyFromLocal ${testSource2} ${sourcePath2}
hdfs dfs -copyFromLocal ${testSource3} ${sourcePath2}

echo "[INFO] FileToHiveTransaction: drop table types on Hive"
hive -e "drop table if exists types;"