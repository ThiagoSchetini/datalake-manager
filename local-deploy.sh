#!/usr/bin/env bash

#-- Variables --#
datalake=~/repository/datalake
manager=${datalake}/datalake-manager
deploy=${datalake}/deploy

#-- Check dirs --#
mkdir -p ${deploy} 2>/dev/null
mkdir -p ${deploy}/resources 2>/dev/null

#-- Clean --#
rm -f ${deploy}/datalake-manager-*-jar-with-dependencies.jar
rm -f ${deploy}/resources/core.properties
rm -f ${deploy}/resources/kerberos.properties
rm -f ${deploy}/resources/log4j.properties
rm -f ${deploy}/resources/spark.properties
rm -f ${deploy}/start-manager.sh

#-- Deploy --#
scp ${manager}/target/datalake-manager-*-jar-with-dependencies.jar ${deploy}
scp ${manager}/src/main/resources/*.properties ${deploy}/resources
scp ${manager}/start-manager.sh ${deploy}