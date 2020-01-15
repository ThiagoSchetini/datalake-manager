package br.com.bvs.datalake.model

import java.sql.Timestamp
import br.com.bvs.datalake.model.property.TransactionProps

case class SmartContract(hash: String,
                         creationTime: Timestamp,
                         smRequester: String,
                         smAuthorizing: String,
                         transactionProps: Option[TransactionProps]) {

  def serializeToCSV: StringBuilder = {
    val delimiter = "#"
    val newline = "\n"
    val smBuilder = new StringBuilder()

    smBuilder.append(
      s"""$hash
         |$creationTime
         |$smRequester
         |$smAuthorizing"""
        .stripMargin.replaceAll(newline, delimiter)).append(newline)

    smBuilder
  }
}