package br.com.bvs.datalake.model.meta

case class SparkMetadata(production: Boolean,
                         search: String,
                         submit: String,
                         mode: String,
                         jar: String,
                         queue: String)
