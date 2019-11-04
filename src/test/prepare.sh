#!/usr/bin/env bash

echo "[INFO] preparing: br.com.bvs.datalake.core.Ernesto.WatchSmartContracts"
smAHDFS=/br/com/bvs/datalake/core/Ernesto/WatchSmartContractsOnA
smBHDFS=/br/com/bvs/datalake/core/Ernesto/WatchSmartContractsOnB
smALocal=src/test/mocks/sm/smA.properties
smBLocal=src/test/mocks/sm/smB.properties
hdfs dfs -mkdir -p ${smAHDFS}
hdfs dfs -mkdir -p ${smBHDFS}
hdfs dfs -copyFromLocal -f ${smALocal} ${smAHDFS}
hdfs dfs -copyFromLocal -f ${smBLocal} ${smBHDFS}