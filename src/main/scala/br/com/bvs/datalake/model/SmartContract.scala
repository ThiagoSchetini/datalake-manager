package br.com.bvs.datalake.model

case class SmartContract(sourceName: String,
                         sourceServer: String,
                         sourcePath: String,
                         sourceFields: List[String],
                         destinationFields: List[String],
                         destinationTypes: List[String],
                         fileReleasePath: String,
                         smartReleasePath: String,
                         distributionPaths: Set[String],
                         versionPattern: String,
                         delimiter: String,
                         header: Boolean,
                         database: String,
                         table: String,
                         overwrite: Boolean)