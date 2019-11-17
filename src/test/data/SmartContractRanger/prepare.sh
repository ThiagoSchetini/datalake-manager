#!/usr/bin/env bash

title="SmartContractRanger test data:"

#-- local --#
test=src/test/data/SmartContractRanger
sm=${test}/placebo.properties

#-- hdfs --#
testHdfs=/br/com/bvs/datalake/core/SmartContractRanger
watch1=${testHdfs}/watch1
watch2=${testHdfs}/watch2
ongoing=${testHdfs}/watch1/ongoing

echo "[TEST] ${title} watch from 2 folders on parallel"
hdfs dfs -rm -R -skipTrash ${watch1} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watch2} 2>/dev/null
hdfs dfs -mkdir -p ${watch1}
hdfs dfs -mkdir -p ${watch2}
hdfs dfs -copyFromLocal ${sm} ${watch1}
hdfs dfs -copyFromLocal ${sm} ${watch2}

echo "[TEST] ${title} look for sm on ongoing dir"
hdfs dfs -rm -R -skipTrash ${ongoing} 2>/dev/null
hdfs dfs -mkdir -p ${ongoing}
hdfs dfs -copyFromLocal ${sm} ${ongoing}