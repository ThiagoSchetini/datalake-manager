#!/usr/bin/env bash

#-- log --#
transaction="FileToHive"
log="[TRANSACTION] ${transaction}:"

#-- local --#
root=src/test/data/FileToHiveTransaction
source1=${root}/source1.csv
source2=${root}/source2.csv
source3=${root}/source3.csv
smPath=${root}/file-to-hive.properties

#-- HDFS --#
transactionHdfsRoot=/br/com/bvs/datalake/transaction/FileToHiveTransaction
transactionHdfsCSV=${transactionHdfsRoot}/file_to_hive
watchSMHdfs=${transactionHdfsRoot}/watchSM
destinyHdfs=${transactionHdfsRoot}/destiny
sourceHdfs=${transactionHdfsRoot}/source

echo "${log} renew HDFS directories"
hdfs dfs -rm -R -skipTrash ${destinyHdfs} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${sourceHdfs} 2>/dev/null
hdfs dfs -rm -R -skipTrash ${watchSMHdfs} 2>/dev/null
hdfs dfs -mkdir -p ${destinyHdfs}
hdfs dfs -mkdir -p ${sourceHdfs}
hdfs dfs -mkdir -p ${watchSMHdfs}
hdfs dfs -mkdir -p ${transactionHdfsCSV}

echo "${log} copy files"
hdfs dfs -copyFromLocal ${source1} ${sourceHdfs}
hdfs dfs -copyFromLocal ${source2} ${sourceHdfs}
hdfs dfs -copyFromLocal ${source3} ${sourceHdfs}
hdfs dfs -copyFromLocal ${smPath} ${watchSMHdfs}

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
fields terminated by '#' \
collection items terminated by ',' \
location '${transactionHdfsCSV}';"