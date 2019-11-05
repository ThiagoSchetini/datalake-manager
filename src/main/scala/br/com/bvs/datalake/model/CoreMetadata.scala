package br.com.bvs.datalake.model

import akka.util.Timeout

final case class CoreMetadata(clientTimeout: Timeout,
                              hadoopConfDir: String,
                              hiveServer2URL: String,
                              hiveServer2PoolFactor: Int,
                              failDirName: String,
                              ongoingDirName: String,
                              doneDirName: String,
                              smWatchDirs: Set[String],
                              smWatchTick: Timeout,
                              smSufix: String,
                              smDelimiter: String)