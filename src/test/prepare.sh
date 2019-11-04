#!/usr/bin/env bash

echo "[INFO] preparing sm.watch.dirs for br.com.bvs.datalake.core.SmartContractRanger"
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
hdfs dfs -mkdir -p ${smBox1Ongoing}
hdfs dfs -mkdir -p ${smBox2Ongoing}
hdfs dfs -copyFromLocal -f ${sm1} ${smBox1}
hdfs dfs -copyFromLocal -f ${sm2} ${smBox2}
hdfs dfs -copyFromLocal -f ${sm3} ${smBox1Ongoing}
hdfs dfs -copyFromLocal -f ${sm4} ${smBox2Ongoing}

echo "[INFO] preparing HIVE for br.com.bvs.datalake.transaction.UserHiveDataTransaction"
TableTypesLocation=/br/com/bvs/datalake/transaction/UserHiveDataTransaction/TableTypes
hdfs dfs -mkdir -p ${TableTypesLocation}
hive -e "create database if not exists testdb;"
hive -e "create external table if not exists testdb.types (\
TINY_NUM TINYINT,\
SMALL_NUM SMALLINT,\
NUM INT,\
BIG_NUM BIGINT,\
FLOAT_NUM FLOAT,\
DOUBLE_NUM DOUBLE,\
DECIMAL_NUM DECIMAL,\
BOOLEAN_FIELD BOOLEAN,\
STRING_FIELD STRING,\
TIMESTAMP_FIELD TIMESTAMP) \
LOCATION hdfs://localhost:9000${TableTypesLocation}"