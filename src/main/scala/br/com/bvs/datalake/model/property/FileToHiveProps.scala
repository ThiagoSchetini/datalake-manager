package br.com.bvs.datalake.model.property

import java.util.Properties

object FileToHiveProps {
  def apply(transactionName: String, props: Properties): FileToHiveProps = {

    new FileToHiveProps(
      transactionName,
      props.getProperty("source.server"),
      props.getProperty("source.path"),
      props.getProperty("source.header").toBoolean,
      props.getProperty("source.delimiter"),
      props.getProperty("source.remove").toBoolean,
      props.getProperty("source.time.format"),
      props.getProperty("destination.path"),
      props.getProperty("destination.database"),
      props.getProperty("destination.table"),
      props.getProperty("destination.fields").split(",").toList,
      props.getProperty("destination.types").split(",").toList,
      props.getProperty("destination.overwrite").toBoolean)
  }
}

case class FileToHiveProps(transactionName: String,
                           sourceServer: String,
                           sourcePath: String,
                           sourceHeader: Boolean,
                           sourceDelimiter: String,
                           sourceRemove: Boolean,
                           sourceTimeFormat: String,
                           destinationPath: String,
                           destinationDatabase: String,
                           destinationTable: String,
                           destinationFields: List[String],
                           destinationTypes: List[String],
                           destinationOverwrite: Boolean) extends TransactionProps