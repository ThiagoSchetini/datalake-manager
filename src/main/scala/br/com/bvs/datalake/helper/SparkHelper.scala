package br.com.bvs.datalake.helper

import br.com.bvs.datalake.model.SubmitMetadata

object SparkHelper {
  private val multiply = 1024
  private val factor = 4

  /* basic */
  private val confFlag = "--conf"
  private val masterFlag = "--master"
  private val master = "yarn"
  private val modeFlag = "--deploy-mode"
  private val queueFlag = "--queue"

  /* driver */
  private val memFlag = "--driver-memory"
  private val coresFlag = "--driver-cores"

  /* executors */
  private val executorsFlag = "--num-executors"
  private val eMemFlag = "--executor-memory"
  private val eCoresFlag = "--executor-cores"

  /* over memory (auto) */
  private val offHeapEnabled = "spark.memory.offHeap.enabled"
  private val offHeapSize = "spark.memory.offHeap.size"
  private val memOverhead = "spark.yarn.driver.memoryOverhead"
  private val eMemOverhead = "spark.yarn.executor.memoryOverhead"

  /* tunning */
  private val resultsFromExecutorsToNoLimits = "spark.driver.maxResultSize=0"
  private val jvmYoungGenTuning = "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5"
  private val useKryoSerializer = "spark.serializer=org.apache.spark.serializer.KryoSerializer"

  /* ethernet stability tolerance */
  private val shuffleParallelConn = "spark.shuffle.io.numConnectionsPerPeer"
  private val yarnMaxRetries = "spark.yarn.maxAppAttempts"
  private val shuffleMaxRetries = "spark.shuffle.io.maxRetries"

  def createSubmit(meta: SubmitMetadata): Seq[String] = {
    val overhead = meta.driverMemory.*(multiply)./(factor)

    Seq(meta.submit,
        masterFlag,     master,
        modeFlag,       meta.mode,
        queueFlag,      meta.queue,
        memFlag,        s"${meta.driverMemory}G",
        coresFlag,      s"${meta.driverCores}",
        executorsFlag,  s"${meta.executors}",
        eMemFlag,       s"${meta.executorMemory}G",
        eCoresFlag,     s"${meta.executorCores}",
        confFlag,       s"$offHeapEnabled=true",
        confFlag,       s"$offHeapSize=$overhead",
        confFlag,       s"$memOverhead=$overhead",
        confFlag,       s"$eMemOverhead=$overhead",
        confFlag,       resultsFromExecutorsToNoLimits,
        confFlag,       jvmYoungGenTuning,
        confFlag,       useKryoSerializer,
        confFlag,       s"$shuffleParallelConn=${meta.shuffleParallelConn}",
        confFlag,       s"$yarnMaxRetries=${meta.retries}",
        confFlag,       s"$shuffleMaxRetries=${meta.retries}",
        meta.jar
    )
  }
}