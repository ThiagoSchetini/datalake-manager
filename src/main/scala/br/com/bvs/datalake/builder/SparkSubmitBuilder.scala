package br.com.bvs.datalake.builder

import br.com.bvs.datalake.model.meta.SubmitMetadata

object SparkSubmitBuilder {
  private val multiply = 1024
  private val factor = 4
  private val master = "yarn"

  /* basic flags */
  private val conf = "--conf"
  private val masterFlag = "--master"
  private val modeFlag = "--deploy-mode"
  private val queueFlag = "--queue"

  /* driver flags */
  private val memFlag = "--driver-memory"
  private val coresFlag = "--driver-cores"

  /* executors flags */
  private val executorsFlag = "--num-executors"
  private val eMemFlag = "--executor-memory"
  private val eCoresFlag = "--executor-cores"

  /* over memory flags */
  private val offHeapEnabled = "spark.memory.offHeap.enabled"
  private val offHeapSize = "spark.memory.offHeap.size"
  private val memOverhead = "spark.yarn.driver.memoryOverhead"
  private val eMemOverhead = "spark.yarn.executor.memoryOverhead"

  /* tunning flags + params */
  private val resultsFromExecutorsToNoLimits = "spark.driver.maxResultSize=0"
  private val jvmYoungGenTuning = "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5"
  private val useKryoSerializer = "spark.serializer=org.apache.spark.serializer.KryoSerializer"

  /* network stability tolerance + throughput (numConnectionsPerPeer) flags */
  private val shuffleParallelConn = "spark.shuffle.io.numConnectionsPerPeer"
  private val yarnMaxRetries = "spark.yarn.maxAppAttempts"
  private val shuffleMaxRetries = "spark.shuffle.io.maxRetries"

  def build(meta: SubmitMetadata): Seq[String] = {
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
        conf,           s"$offHeapEnabled=true",
        conf,           s"$offHeapSize=$overhead",
        conf,           s"$memOverhead=$overhead",
        conf,           s"$eMemOverhead=$overhead",
        conf,           resultsFromExecutorsToNoLimits,
        conf,           jvmYoungGenTuning,
        conf,           useKryoSerializer,
        conf,           s"$shuffleParallelConn=${meta.shuffleParallelConn}",
        conf,           s"$yarnMaxRetries=${meta.retries}",
        conf,           s"$shuffleMaxRetries=${meta.retries}",
        meta.jar,
        meta.source,
        meta.destiny,
        meta.overwrite.toString,
        meta.pipeline,
        meta.types,
        meta.dateFormat,
        meta.header.toString,
        meta.fields,
        meta.delimiter
    )
  }
}
