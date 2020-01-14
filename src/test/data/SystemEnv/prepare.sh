#!/usr/bin/env bash

logsys="[SYSTEM] System:"
logsm="[SYSTEM] SmartContract:"
testHdfsDir=/smartcontract

#-- local --#
echo "${logsys} checking signals system directory"
mkdir target/signals 2>/dev/null
touch target/signals/package.signal

#-- HDFS --#
echo "${logsys} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${testHdfsDir} 2>/dev/null
hdfs dfs -mkdir -p ${testHdfsDir}

#-- Hive --#
echo "${logsys} checking database: testdb"
hive -e "create database if not exists testdb;"

echo "${logsm} renew smartcontract Hive table"
hive -e "drop table if exists testdb.smartcontract"

hive -e \
"create external table if not exists testdb.smartcontract (\
HASH STRING,\
CREATION_TIME TIMESTAMP,\
REQUESTER STRING,\
AUTHORIZING STRING) \
row format delimited \
fields terminated by '|' \
collection items terminated by ',' \
location '/smartcontract';"