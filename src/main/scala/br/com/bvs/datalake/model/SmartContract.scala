package br.com.bvs.datalake.model

case class SmartContract(sourceServer: String,
                         sourcePath: String,
                         sourceHeader: Boolean,
                         sourceDelimiter: String,
                         sourceRemove: Boolean,
                         sourceTimeFormat: String,
                         destinationFields: List[String],
                         destinationTypes: List[String],
                         destinationPath: String,
                         destinationDatabase: String,
                         destinationTable: String,
                         destinationOverwrite: Boolean,
                         transaction: String,
                         requester: String,
                         authorizing: String)