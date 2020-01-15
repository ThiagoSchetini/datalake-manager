#!/usr/bin/env bash

#-- log --#
log="[SYSTEM] SmartContract:"

#-- local --#
testroot=src/test/data/SystemEnv

echo "${log} checking signals system directory"
mkdir target/signals 2>/dev/null

#-- HDFS --#
smHdfsRoot=/br/com/bvs/datalake/model/SmartContract
smHdfsCSV=${smHdfsRoot}/smart_contract

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${smHdfsRoot} 2>/dev/null
hdfs dfs -mkdir -p ${smHdfsCSV}

#-- Hive --#
echo "${log} checking Hive database"
hive -e "create database if not exists datalake_manager;"

echo "${log} renew Hive table"
hive -e "drop table if exists datalake_manager.smart_contract"

hive -e \
"create external table if not exists datalake_manager.smart_contract (\
HASH STRING,\
CREATION_TIME TIMESTAMP,\
REQUESTER STRING,\
AUTHORIZING STRING) \
row format delimited \
fields terminated by '#' \
collection items terminated by ',' \
location '${smHdfsCSV}';"