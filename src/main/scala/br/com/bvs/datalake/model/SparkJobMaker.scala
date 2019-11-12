package br.com.bvs.datalake.model

sealed trait SparkJob

case class SparkJobDeveloper(s: String) extends SparkJob

case class SparkJobProduction(queue: String,
                              driverMemory: Int,
                              driverCores: Int,
                              numExecutors: Int,
                              executorMemory: Int,
                              executorCores: Int,
                              shuffleParallelConn: Int,
                              yarnMaxRetries: Int,
                              shuffleMaxRetries: Int) extends SparkJob

class SparkJobMaker {
  private val multiplyBase = 1024
  private val overheadFactor = 4

  private val basic = "spark-submit --master yarn"
  private val mode = "--deploy-mode "
  private val queue = "--queue "

  /* driver */
  private val driverMemory = "--driver-memory "
  private val driverCores = "--driver-cores "

  /* executors */
  private val numExecutors = "--num-executors "
  private val executorMemory = "--executor-memory "
  private val executorCores = "--executor-cores "

  /* over memory (auto) */
  private val offHeapEnable = "--conf spark.memory.offHeap.enabled=true"
  private val offHeapSize = "--conf spark.memory.offHeap.size="
  private val driverMemoryOverhead = "--conf spark.yarn.driver.memoryOverhead="
  private val executorMemoryOverhead = "--conf spark.yarn.executor.memoryOverhead="

  /* tunning */
  private val noDriverLimit = "--conf spark.driver.maxResultSize=0"
  private val jvmElasticYoungGen = "--conf spark.executor.extraJavaOptions=\"-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5\""
  private val forceKryoSerializer = "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
  private val shuffleParallelConn = "--conf spark.shuffle.io.numConnectionsPerPeer="

  /* ethernet stability tolerance */
  private val yarnMaxRetries = "--conf spark.yarn.maxAppAttempts="
  private val shuffleMaxRetries = "--conf spark.shuffle.io.maxRetries="

  def make(t: SparkJob): Any = t match {
    case SparkJobDeveloper(s) =>
      println("developer detected")
      "developer"

    case SparkJobProduction(q, memory, cores, executors, memoryex, coresex, shuffle, retries, shuffleRetries) =>
      println("fdsa")
      "production"
  }
}

/*
spark-submit \
--master yarn \
--deploy-mode cluster \
--queue root.svc_datalake \
--driver-memory 24G \
--driver-cores 12 \
--conf spark.yarn.driver.memoryOverhead=8192 \
--num-executors 3 \
--executor-memory 16G \
--executor-cores 12 \
--conf spark.yarn.executor.memoryOverhead=8192 \
--conf spark.driver.maxResultSize=0 \
--conf spark.shuffle.io.numConnectionsPerPeer=3 \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=8192 \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5" \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.yarn.maxAppAttempts=15 \
--conf spark.shuffle.io.maxRetries=15 \
 */
