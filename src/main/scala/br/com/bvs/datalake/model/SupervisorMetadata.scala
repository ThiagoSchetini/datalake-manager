package br.com.bvs.datalake.model

import akka.util.Timeout

final case class SupervisorMetadata(ongoingDirectory: String,
                                    failDirectory: String,
                                    hdfsClientTimeout: Timeout)