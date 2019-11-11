#!/usr/bin/env bash

testPath=src/test/data/FileToHiveTransaction
hdfsPath=/br/com/bvs/datalake/transaction/FileToHiveTransaction

echo "[INFO] FileToHiveTransaction: prepare sm's to watched folders"
sm1=${testPath}/sm1.properties
sm2=${testPath}/sm2.properties
watch1=${hdfsPath}/watch1
hdfs dfs -rm -R -skipTrash ${watch1} 2>/dev/null
hdfs dfs -mkdir -p ${watch1}
hdfs dfs -copyFromLocal ${sm1} ${watch1}
hdfs dfs -copyFromLocal ${sm2} ${watch1}

echo "[INFO] FileToHiveTransaction: prepare destiny"
destinationPath=${hdfsPath}/destiny
hdfs dfs -rm -R -skipTrash ${destinationPath} 2>/dev/null
hdfs dfs -mkdir -p ${destinationPath}

echo "[INFO] FileToHiveTransaction: prepare single file source"
sourcePath1=${hdfsPath}/source1/file1.csv
hdfs dfs -rm -R -skipTrash ${sourcePath1} 2>/dev/null
hdfs dfs -mkdir -p ${sourcePath1}
testSource1=${testPath}/file1.csv
hdfs dfs -copyFromLocal ${testSource1} ${sourcePath1}

echo "[INFO] FileToHiveTransaction: prepare directory with sources"
sourcePath2=${hdfsPath}/source2
hdfs dfs -rm -R -skipTrash ${sourcePath2} 2>/dev/null
hdfs dfs -mkdir -p ${sourcePath2}
testSource2=${testPath}/file2.csv
hdfs dfs -copyFromLocal ${testSource2} ${sourcePath2}
testSource3=${testPath}/file3.csv
hdfs dfs -copyFromLocal ${testSource3} ${sourcePath2}

hive -e "drop table if exists types;"