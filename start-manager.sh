#!/usr/bin/env bash

deploy=~/repository/datalake/deploy/
export DATALAKE_MANAGER_PROPS=${deploy}/resources

#-- move to deploy --#

cd ${deploy}

java \
-XX:+UseG1GC \
-Xmx8G \
-XX:NewRatio=1 \
-XX:SurvivorRatio=128 \
-XX:MinHeapFreeRatio=5 \
-XX:MaxHeapFreeRatio=5 \
-jar ${deploy}/datalake-manager-0.1-jar-with-dependencies.jar