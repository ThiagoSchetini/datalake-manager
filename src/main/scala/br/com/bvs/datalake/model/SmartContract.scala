package br.com.bvs.datalake.model

import java.sql.Timestamp
import br.com.bvs.datalake.util.TextUtil

case class SmartContract(hash: String,
                         creationTime: Timestamp,
                         requester: String,
                         authorizing: String,
                         fileName: String,
                         transaction: String,
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
                         destinationOverwrite: Boolean)
 {

  def serializeCSV: StringBuilder = {
    val newline = "\n"
    val smBuilder = new StringBuilder()

    smBuilder.append(
      s"""$hash
         |$creationTime
         |$requester
         |$authorizing
         |$fileName
         |$transaction
         |$sourceServer
         |$sourcePath
         |$destinationPath
         |$destinationDatabase
         |$destinationTable
         |${TextUtil.serializeList(destinationFields)}
         |${TextUtil.serializeList(destinationTypes)}
         |$destinationOverwrite"""
        .stripMargin.replaceAll(newline, "|")).append(newline)

    smBuilder
  }

}