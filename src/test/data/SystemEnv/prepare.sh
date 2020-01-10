#!/usr/bin/env bash

log="[SYSTEM] SmartContract:"

#-- hdfs --#
testHdfsSM=/smartcontract

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${testHdfsSM} 2>/dev/null
hdfs dfs -mkdir -p ${testHdfsSM}

echo "${log} renew SmartContract Hive table"
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

echo "${log} checking signals system directory"
mkdir target/signals 2>/dev/null
touch target/signals/package.signal