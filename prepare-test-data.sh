#!/usr/bin/env bash

echo "[INFO] Preparing environment for tests ... "
testroot=src/test/data

echo "[INFO] Database: testdb"
hive -e "create database if not exists testdb;"

sh ${testroot}/SmartContractRanger/prepare.sh
sh ${testroot}/FileToHiveTransaction/prepare.sh