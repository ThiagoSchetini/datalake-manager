#!/usr/bin/env bash

echo "[TEST] SmartContract: hive smart contract table"

#-- hdfs --#
testHdfsSM=/smartcontract

hdfs dfs -rm -R -skipTrash ${testHdfsSM} 2>/dev/null
hdfs dfs -mkdir -p ${testHdfsSM}
hive -e "drop table if exists testdb.smartcontract"

hive -e \
"create external table if not exists testdb.smartcontract (\
HASH STRING,\
CREATION_TIME TIMESTAMP,\
REQUESTER STRING,\
AUTHORIZING STRING,\
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

echo "[TEST] Shutdown Signal: prepare directory"
mkdir target/signals 2>/dev/null
touch target/signals/package.signal