#!/usr/bin/env bash

smTitle="SmartContract:"

#-- hdfs --#
testHdfs=/smartcontract

echo "[TEST] ${smTitle} hdfs destiny dir for all sm's"
hdfs dfs -rm -R -skipTrash ${testHdfs} 2>/dev/null
hdfs dfs -mkdir -p ${testHdfs}

echo "[TEST] ${smTitle} hive smart contract table"

#hive -e "drop table if exists testdb.smartcontract"

hive -e \
"create external table if not exists testdb.smartcontract (\
HASH STRING,\
CREATION_TIME TIMESTAMP,\
FILENAME STRING,\
TRANSACTION STRING,\
SRC_SERVER STRING,\
SRC_PATH STRING,\
DESTINATION_PATH STRING,\
DESTINATION_DATABASE STRING,\
DESTINATION_TABLE STRING,\
DESTINATION_FIELDS ARRAY<STRING>,\
DESTINATION_TYPES ARRAY<STRING>,\
DESTINATION_OVERWRITE BOOLEAN) \
row format delimited \
fields terminated by '|' \
collection items terminated by ',' \
location '/smartcontract';"

mkdir target/shutdown-signal 2>/dev/null