package br.com.bvs.datalake.model

case class SmartContract(sourceServer: String,
                         sourcePath: String,
                         sourceHeader: Boolean,
                         sourceDelimiter: String,
                         sourceBooleanTrue: String,
                         sourceBooleanFalse: String,
                         sourceTimeFormat: String,
                         destinationFields: List[String],
                         destinationTypes: List[String],
                         destinationPath: String,
                         destinationDatabase: String,
                         destinationTable: String,
                         destinationOverwrite:Boolean)