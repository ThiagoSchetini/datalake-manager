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
echo "${logsys} checking Hive database"
hive -e "create database if not exists datalake_manager;"

echo "${logsm} renew Hive table"
hive -e "drop table if exists datalake_manager.smart_contract"

hive -e \
"create external table if not exists datalake_manager.smart_contract (\
HASH STRING,\
CREATION_TIME TIMESTAMP,\
REQUESTER STRING,\
AUTHORIZING STRING) \
row format delimited \
fields terminated by '|' \
collection items terminated by ',' \
location '/br/com/bvs/datalake/model/SmartContract/smart_contract';"