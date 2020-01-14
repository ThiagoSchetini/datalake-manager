package br.com.bvs.datalake.model.meta

import akka.util.Timeout

final case class CoreMetadata(clientTimeout: Timeout,
                              hadoopConfDir: String,
                              hiveServer2URL: String,
                              hiveServer2PoolFactor: Int,
                              smHdfsWatch: Set[String],
                              smWatchTick: Timeout,
                              smSufix: String,
                              shutdownSignalDir: String,
                              shutdownSignalFile: String,
                              shutdownWatchTick: Timeout,
                              failDirName: String,
                              ongoingDirName: String,
                              doneDirName: String,
                              smHdfsDestiny: String,
                              fileToHiveHdfsDestiny: String)