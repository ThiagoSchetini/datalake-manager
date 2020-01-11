package br.com.bvs.datalake.model.meta

case class SubmitMetadata(submit: String,
                          mode: String,
                          jar: String,
                          queue: String,
                          driverMemory: Int,
                          driverCores: Int,
                          executors: Int,
                          executorMemory: Int,
                          executorCores: Int,
                          shuffleParallelConn: Int,
                          retries: Int,
                          source: String,
                          destiny: String,
                          overwrite: Boolean,
                          pipeline: String,
                          types: String,
                          dateFormat: String,
                          fields: String,
                          header: Boolean,
                          delimiter: String)