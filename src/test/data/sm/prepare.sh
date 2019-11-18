#!/usr/bin/env bash

title="SmartContract:"

#-- hdfs --#
testHdfs=/smartcontract

echo "[TEST] ${title} hdfs destiny dir for all sm's"
#hdfs dfs -rm -R -skipTrash ${testHdfs} 2>/dev/null
hdfs dfs -mkdir -p ${testHdfs}

echo "[TEST] ${title} hive smart contract table"

hive -e "drop table if exists testdb.smartcontract"

hive -e \
"create external table testdb.smartcontract (\
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
location '/smartcontract' \
row format delimited \
fields terminated by '|' \
collection items terminated by ',';"