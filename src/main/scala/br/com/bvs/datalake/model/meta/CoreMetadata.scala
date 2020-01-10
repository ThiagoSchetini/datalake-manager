package br.com.bvs.datalake.model.meta

import akka.util.Timeout

final case class CoreMetadata(clientTimeout: Timeout,
                              hadoopConfDir: String,
                              hiveServer2URL: String,
                              hiveServer2PoolFactor: Int,
                              failDirName: String,
                              ongoingDirName: String,
                              doneDirName: String,
                              smWatchHdfsDirs: Set[String],
                              smWatchTick: Timeout,
                              smSufix: String,
                              smDestinyHdfsDir: String,
                              shutdownSignalLocalDir: String,
                              shutdownSignalFile: String,
                              shutdownWatchTick: Timeout)
