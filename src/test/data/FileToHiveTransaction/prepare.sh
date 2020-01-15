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
sourcePath=${transactionTestPath}/source

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${destinyPath} 2>/dev/null
hdfs dfs -mkdir -p ${destinyPath}

echo "${log} copy files"
hdfs dfs -rm -R -skipTrash ${sourcePath} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchSMPath} 2>/dev/null
hdfs dfs -mkdir -p ${sourcePath}
hdfs dfs -mkdir -p ${watchSMPath}
hdfs dfs -copyFromLocal ${source1} ${sourcePath}
hdfs dfs -copyFromLocal ${source2} ${sourcePath}
hdfs dfs -copyFromLocal ${source3} ${sourcePath}
hdfs dfs -copyFromLocal ${smPath} ${watchSMPath}

#-- Hive --#
echo "${log} drop Hive table"
hive -e "drop table if exists datalake_manager.types;"

echo "${log} renew Hive table"
hive -e "drop table if exists datalake_manager.file_to_hive"

hive -e \
"create external table if not exists datalake_manager.file_to_hive (\
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
location '/br/com/bvs/datalake/model/SmartContract/file_to_hive';"