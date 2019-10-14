#!/usr/bin/env bash

export DATALAKE_MANAGER=~/test/datalakemanager
export DATALAKE_MANAGER_PROPS=${DATALAKE_MANAGER}/properties

java \
-XX:+UseG1GC \
-Xmx12G \
-XX:NewRatio=1 \
-XX:SurvivorRatio=128 \
-XX:MinHeapFreeRatio=5 \
-XX:MaxHeapFreeRatio=5 \
-jar ${DATALAKE_MANAGER}/datalake-manager-0.1-jar-with-dependencies.jar