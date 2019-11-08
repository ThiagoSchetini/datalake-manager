#!/usr/bin/env bash

echo "[INFO] Preparing environment for tests ... "

echo "[INFO] Database: testdb"
hive -e "drop database if exists testdb;"
hive -e "create database testdb;"

sh SmartContractRanger/prepare.sh
sh FileToHiveTransaction/prepare.sh