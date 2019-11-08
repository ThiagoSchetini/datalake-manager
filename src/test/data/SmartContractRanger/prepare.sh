#!/usr/bin/env bash

testPath=src/test/data/SmartContractRanger
hdfsPath=/br/com/bvs/datalake/core/SmartContractRanger

echo "[INFO] SmartContractRanger: prepare to watch from folders"
sm1=${testPath}/sm1.properties
sm2=${testPath}/sm2.properties
watch1=${hdfsPath}/watch1
watch2=${hdfsPath}/watch2
hdfs dfs -rm -R -skipTrash ${watch1}
hdfs dfs -rm -R -skipTrash ${watch2}
hdfs dfs -mkdir -p ${watch1}
hdfs dfs -mkdir -p ${watch2}
hdfs dfs -copyFromLocal ${sm1} ${watch1}
hdfs dfs -copyFromLocal ${sm2} ${watch2}

echo "[INFO] SmartContractRanger: prepare to watch from ongoing folders"
sm3=${testPath}/sm3.properties
sm4=${testPath}/sm4.properties
ongoing1=${hdfsPath}/watch1/ongoing
ongoing2=${hdfsPath}/watch2/ongoing
hdfs dfs -rm -R -skipTrash ${ongoing1}
hdfs dfs -rm -R -skipTrash ${ongoing2}
hdfs dfs -mkdir -p ${ongoing1}
hdfs dfs -mkdir -p ${ongoing2}
hdfs dfs -copyFromLocal ${sm3} ${ongoing1}
hdfs dfs -copyFromLocal ${sm4} ${ongoing2}