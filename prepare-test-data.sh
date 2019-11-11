#!/usr/bin/env bash

echo "[INFO] Preparing environment for tests ... "

echo "[INFO] Database: testdb"
hive -e "create database if not exists testdb;"

sh src/test/data/SmartContractRanger/prepare.sh
sh src/test/data/FileToHiveTransaction/prepare.sh