package br.com.bvs.datalake.model

class SparkSubmitBuilder {
  private val basic = "spark-submit --master yarn --deploy-mode cluster"
  private val queue = "--queue "
  private var queueValue: String = _

  /* driver */
  private val driverMemory = "--driver-memory "
  private val driverCores = "--driver-cores "

  /* executors */
  private val numExecutors = "--num-executors "
  private val executorMemory = "--executor-memory "
  private val executorCores = "--executor-cores "

  /* over memory */
  private val offHeapEnable = "--conf spark.memory.offHeap.enabled=true"
  private val offHeapSize = "--conf spark.memory.offHeap.size="
  private val driverMemoryOverhead = "--conf spark.yarn.driver.memoryOverhead="
  private val executorMemoryOverhead = "--conf spark.yarn.executor.memoryOverhead="

  /* tunning */
  private val noDriverLimit = "--conf spark.driver.maxResultSize=0"
  private val jvmElasticYoungGen = "--conf spark.executor.extraJavaOptions=\"-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5\""
  private val forceKryoSerializer = "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
  private val shuffleConnections = "--conf spark.shuffle.io.numConnectionsPerPeer="

  /* ethernet stability tolerance */
  private val yarnMaxRetries = "--conf spark.yarn.maxAppAttempts="
  private val shuffleMaxRetries = "--conf spark.shuffle.io.maxRetries="
}

/*
spark-submit \
--master yarn \
--deploy-mode cluster \
--queue root.svc_datalake \
--driver-memory 32G \
--driver-cores 16 \
--conf spark.yarn.driver.memoryOverhead=16384 \
--num-executors 12 \
--executor-memory 24G \
--executor-cores 12 \
--conf spark.yarn.executor.memoryOverhead=16384 \
--conf spark.driver.maxResultSize=0 \
--conf spark.shuffle.io.numConnectionsPerPeer=10 \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=16384 \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5" \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.yarn.maxAppAttempts=5 \
--conf spark.shuffle.io.maxRetries=5 \
 */
