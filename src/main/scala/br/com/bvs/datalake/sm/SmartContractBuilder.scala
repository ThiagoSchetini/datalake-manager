package br.com.bvs.datalake.sm

import java.util.Properties

class SmartContractBuilder(props: Properties) {
  def build: SmartContract = {
    SmartContract(props.getProperty("sourceName"),
                  props.getProperty("sourceServer"),
                  props.getProperty("sourcePath"),
                  props.getProperty("sourceFields").split(",").toSet,
                  props.getProperty("destinationFields").split(",").toSet,
                  props.getProperty("destinationTypes").split(",").toSet,
                  props.getProperty("fileReleasePath"),
                  props.getProperty("smartReleasePath"),
                  props.getProperty("distributionPaths").split(",").toSet,
                  props.getProperty("versionPattern"),
                  props.getProperty("delimiter"),
                  props.getProperty("header")

    )
  }

}

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

