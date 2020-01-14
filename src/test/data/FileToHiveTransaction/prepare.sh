#!/usr/bin/env bash

transaction="FileToHive"
log="[DATA] Transaction ${transaction}:"

#-- local --#
test=src/test/data/FileToHiveTransaction
source1=${test}/source1.csv
source2=${test}/source2.csv
source3=${test}/source3.csv
smPath=${test}/file-to-hive.properties

#-- HDFS --#
transactionTestPath=/br/com/bvs/datalake/transaction/FileToHiveTransaction
watchSMPath=${transactionTestPath}/watchSM
destinyPath=${transactionTestPath}/destiny
sourcesPath=${transactionTestPath}/sources

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${destinyPath} 2>/dev/null
hdfs dfs -mkdir -p ${destinyPath}

echo "${log} copy files"
hdfs dfs -rm -R -skipTrash ${sourcesPath} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchSMPath} 2>/dev/null
hdfs dfs -mkdir -p ${sourcesPath}
hdfs dfs -mkdir -p ${watchSMPath}
hdfs dfs -copyFromLocal ${source1} ${sourcesPath}
hdfs dfs -copyFromLocal ${source2} ${sourcesPath}
hdfs dfs -copyFromLocal ${source3} ${sourcesPath}
hdfs dfs -copyFromLocal ${smPath} ${watchSMPath}

#-- Hive --#
echo "${log} drop Hive table"
hive -e "drop table if exists testdb.types;"

echo "${log} renew tr_filetohive Hive table"
hive -e "drop table if exists testdb.tr_filetohive"

hive -e \
"create external table if not exists testdb.tr_filetohive (\
SM_HASH STRING,\
TRANSACTION STRING,\
SRC_SERVER STRING,\
SRC_PATH STRING,\
SRC_HEADER BOOLEAN,\
SRC_DELIMITER STRING,\
SRC_REMOVE BOOLEAN,\
SRC_TIME_FORMAT STRING,\
DEST_PATH STRING,\
DEST_DATABASE STRING,\
DEST_TABLE STRING,\
DEST_FIELDS ARRAY<STRING>,\
DEST_TYPES ARRAY<STRING>,\
DEST_OVERWRITE BOOLEAN) \
row format delimited \
fields terminated by '|' \
collection items terminated by ',' \
location '/smartcontract/filetohive';"