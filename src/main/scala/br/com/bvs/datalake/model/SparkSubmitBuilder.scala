package br.com.bvs.datalake.model

class SparkSubmitBuilder {
  private val basicFlags = "spark-submit --master yarn --deploy-mode cluster"
  private val queueFlag = "--queue "


  /* driver */
  private val driverMemoryFlag = "--driver-memory "
  private val driverCoresFlag = "--driver-cores "
  private val driverMemoryOverheadFlag = "--conf spark.yarn.driver.memoryOverhead="

  /* executors */
  private val numExecutorsFlag = "--num-executors "
  private val executorMemoryFlag = "--executor-memory "
  private val executorCoresFlag = "--executor-cores "
  private val executorMemoryOverheadFlag = "--conf spark.yarn.executor.memoryOverhead="

  /* tunning */
  private val driverNoLimit = "--conf spark.driver.maxResultSize=0"
  private val jvmElasticYoungGen = "--conf spark.executor.extraJavaOptions=\"-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5\""


}


/*


--conf spark.shuffle.io.numConnectionsPerPeer=10 \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=16384 \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5" \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.yarn.maxAppAttempts=5 \
--conf spark.shuffle.io.maxRetries=5 \
 */


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
