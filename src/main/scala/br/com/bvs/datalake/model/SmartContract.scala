package br.com.bvs.datalake.model

case class SmartContract(sourceName: String,
                         sourceServer: String,
                         sourcePath: String,
                         sourceFields: Set[String],
                         destinationFields: Set[String],
                         destinationTypes: Set[String],
                         smartReleasePath: String,
                         fileReleasePath: String,
                         distributionPaths: Set[String],
                         versionPattern: String,
                         delimiter: String,
                         header: String)