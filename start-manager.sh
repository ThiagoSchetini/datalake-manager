#!/usr/bin/env bash

export DATALAKE_MANAGER_PROPS=src/main/resources
export DATALAKE_SPARK_HOME=~/repository/datalake/datalake-spark
export DATALAKE_SPARK_PROPS=${DATALAKE_SPARK_HOME}/src/main/resources
export DATALAKE_SPARK_JARS=${DATALAKE_SPARK_HOME}/target

java \
-XX:+UseG1GC \
-Xmx8G \
-XX:NewRatio=1 \
-XX:SurvivorRatio=128 \
-XX:MinHeapFreeRatio=5 \
-XX:MaxHeapFreeRatio=5 \
-jar target/datalake-manager-0.1-jar-with-dependencies.jar