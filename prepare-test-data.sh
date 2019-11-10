#!/usr/bin/env bash

echo "[INFO] Preparing environment for tests ... "

echo "[INFO] Database: testdb"
hive -e "drop database if exists testdb;"
hive -e "create database testdb;"

sh src/test/data/SmartContractRanger/prepare.sh
sh src/test/data/FileToHiveTransaction/prepare.sh