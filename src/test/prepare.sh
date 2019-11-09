#!/usr/bin/env bash

echo "[INFO] preparing files: br.com.bvs.datalake.core.SmartContractRanger"
smBox1=/br/com/bvs/datalake/core/SmartContractRanger/SmartContractsBox1
smBox2=/br/com/bvs/datalake/core/SmartContractRanger/SmartContractsBox2
smBox1Ongoing=/br/com/bvs/datalake/core/SmartContractRanger/SmartContractsBox1/ongoing
smBox2Ongoing=/br/com/bvs/datalake/core/SmartContractRanger/SmartContractsBox2/ongoing
sm1=src/test/mocks/sm/sm1.properties
sm2=src/test/mocks/sm/sm2.properties
sm3=src/test/mocks/sm/sm3.properties
sm4=src/test/mocks/sm/sm4.properties
hdfs dfs -mkdir -p ${smBox1}
hdfs dfs -mkdir -p ${smBox2}
hdfs dfs -rm -R -skipTrash ${smBox1Ongoing}
hdfs dfs -rm -R -skipTrash ${smBox2Ongoing}
hdfs dfs -mkdir -p ${smBox1Ongoing}
hdfs dfs -mkdir -p ${smBox2Ongoing}
hdfs dfs -copyFromLocal -f ${sm1} ${smBox1}
hdfs dfs -copyFromLocal -f ${sm2} ${smBox2}
hdfs dfs -copyFromLocal -f ${sm3} ${smBox1Ongoing}
hdfs dfs -copyFromLocal -f ${sm4} ${smBox2Ongoing}

echo "[INFO] preparing Hive: br.com.bvs.datalake.transaction.UserHiveDataTransaction"
TableProductsLocation=/br/com/bvs/datalake/transaction/UserHiveDataTransaction/Products
hive -e "drop database if exists testdb;"
hdfs dfs -rm -R -skipTrash ${TableProductsLocation}
hdfs dfs -mkdir -p ${TableProductsLocation}