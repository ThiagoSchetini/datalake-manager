#!/usr/bin/env bash

echo "[INFO] preparing env for integration test"

if [[ ! -d ~/test/datalakemanager ]]; then
    mkdir -p ~/test/datalakemanager
    mkdir -p ~/test/datalakemanager/properties
fi
export DATALAKE_MANAGER=~/test/datalakemanager
export DATALAKE_MANAGER_PROPS=${DATALAKE_MANAGER}/properties

rm -r ${DATALAKE_MANAGER_PROPS}/* 2>/dev/null
rm ${DATALAKE_MANAGER}/*jar-with-dependencies.jar 2>/dev/null
mkdir -p ${DATALAKE_MANAGER}

echo "[INFO] copying .jar, properties and test sources"
cp src/main/resources/*.properties ${DATALAKE_MANAGER_PROPS}
cp target/*jar-with-dependencies.jar ${DATALAKE_MANAGER}
cp execute.sh ${DATALAKE_MANAGER}

java \
-XX:+UseG1GC \
-Xmx2G \
-XX:NewRatio=1 \
-XX:SurvivorRatio=128 \
-XX:MinHeapFreeRatio=5 \
-XX:MaxHeapFreeRatio=5 \
-jar ${DATALAKE_MANAGER}/datalake-manager-0.1-jar-with-dependencies.jar