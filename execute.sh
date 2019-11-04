#!/usr/bin/env bash

export DATALAKE_MANAGER_PROPS=src/main/resources

java \
-XX:+UseG1GC \
-Xmx4G \
-XX:NewRatio=1 \
-XX:SurvivorRatio=128 \
-XX:MinHeapFreeRatio=5 \
-XX:MaxHeapFreeRatio=5 \
-jar target/datalake-manager-0.1-jar-with-dependencies.jar