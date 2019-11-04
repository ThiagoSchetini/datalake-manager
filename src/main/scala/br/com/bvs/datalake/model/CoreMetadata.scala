package br.com.bvs.datalake.model

import akka.util.Timeout

final case class CoreMetadata(hadoopConfDir: String,
                              hdfsClientTimeout: Timeout,
                              failDirName: String,
                              ongoingDirName: String,
                              smWatchDirs: Set[String],
                              smWatchTick: Timeout,
                              smSufix: String,
                              smDelimiter: String)