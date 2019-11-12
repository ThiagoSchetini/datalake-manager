package br.com.bvs.datalake.helper

sealed trait SparkJob

case class SparkJobDeveloper(jar: String) extends SparkJob

case class SparkJobProduction(jar: String,
                              queue: String,
                              driverMemory: Int,
                              driverCores: Int,
                              executors: Int,
                              executorMemory: Int,
                              executorCores: Int,
                              shuffleParallelConn: Int,
                              retries: Int) extends SparkJob

object SparkJobHelper {
  private val multiplyBytes = 1024
  private val overheadFactor = 4
  private val sep = " "

  /* basic info */
  private val basic = "spark-submit --master yarn"
  private val mode = "--deploy-mode"
  private val queue = "--queue"

  /* driver */
  private val driverMemory = "--driver-memory"
  private val driverCores = "--driver-cores"

  /* executors */
  private val numExecutors = "--num-executors"
  private val executorMemory = "--executor-memory"
  private val executorCores = "--executor-cores"

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

  def serializeSubmit(t: SparkJob): StringBuilder = t match {
    case SparkJobDeveloper(jar) =>
      val builder = new StringBuilder
      builder.append(s"$basic$sep")
      builder.append(s"$mode client$sep")
      builder.append(jar)

    case SparkJobProduction(jar, q, mem, cores, executors, eMem, eCores, connections, retries) =>
      val overheadMemory = mem.*(multiplyBytes)./(overheadFactor)
      val builder = new StringBuilder
      builder.append(s"$basic$sep")
      builder.append(s"$mode cluster$sep")
      builder.append(s"$queue $q$sep")
      builder.append(s"$driverMemory ${mem}G$sep")
      builder.append(s"$driverCores $cores$sep")
      builder.append(s"$numExecutors $executors$sep")
      builder.append(s"$executorMemory ${eMem}G$sep")
      builder.append(s"$executorCores $eCores$sep")
      builder.append(s"$offHeapEnable$sep")
      builder.append(s"$offHeapSize$overheadMemory$sep")
      builder.append(s"$driverMemoryOverhead$overheadMemory$sep")
      builder.append(s"$executorMemoryOverhead$overheadMemory$sep")
      builder.append(s"$noDriverLimit$sep")
      builder.append(s"$jvmElasticYoungGen$sep")
      builder.append(s"$forceKryoSerializer$sep")
      builder.append(s"$shuffleParallelConn$connections$sep")
      builder.append(s"$yarnMaxRetries$retries$sep")
      builder.append(s"$shuffleMaxRetries$retries$sep")
      builder.append(jar)
  }

}