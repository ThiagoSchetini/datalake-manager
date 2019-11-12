package br.com.bvs.datalake.model

sealed trait SubmitMetadata

case class Developer(submit: String,
                     mode: String,
                     jar: String) extends SubmitMetadata

case class Production(submit: String,
                      mode: String,
                      jar: String,
                      queue: String,
                      driverMemory: Int,
                      driverCores: Int,
                      executors: Int,
                      executorMemory: Int,
                      executorCores: Int,
                      shuffleParallelConn: Int,
                      retries: Int) extends SubmitMetadata