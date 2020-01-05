package br.com.bvs.datalake.core

import java.io.ByteArrayInputStream
import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, Properties}
import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import br.com.bvs.datalake.core.SmartContractBuilder.{Build, Built}
import br.com.bvs.datalake.model.SmartContract
import org.apache.hadoop.fs.Path

object SmartContractBuilder {
  def props: Props = Props(new SmartContractBuilder)

  case class Build(smData: String, ongoingPath: Path)
  case class Built(sm: SmartContract, ongoingPath: Path)
}

class SmartContractBuilder extends Actor {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {
    case Build(smData, ongoingPath) =>
      val sm = onBuild(smData, ongoingPath)
      val check = validate(sm)

      if (!check) {
        //onTransactionFail(smPath, "sm not valid")
        //TODO create the protocol and send an invalid or NotBuilt
      } else {
        sender ! Built(sm, ongoingPath)
      }
  }

  private def onBuild(smData: String, ongoingPath: Path): SmartContract = {
    val props = new Properties()
    props.load(new ByteArrayInputStream(smData.getBytes()))

    val creationTime = new Timestamp(new Date().getTime)
    val requester = props.getProperty("requester")
    val authorizing = props.getProperty("authorizing")
    val filename = ongoingPath.getName
    val transaction = props.getProperty("transaction")
    val sourceServer = props.getProperty("source.server")
    val sourcePath = props.getProperty("source.path")
    val sourceHeader = props.getProperty("source.header").toBoolean
    val sourceDelimiter = props.getProperty("source.delimiter")
    val sourceRemove = props.getProperty("source.remove").toBoolean
    val sourceTimeFormat = props.getProperty("source.time.format")
    val destinationPath = props.getProperty("destination.path")
    val destinationDatabase = props.getProperty("destination.database")
    val destinationTable = props.getProperty("destination.table")
    val destinationFields = props.getProperty("destination.fields").split(",").toList
    val destinationTypes = props.getProperty("destination.types").split(",").toList
    val destinationOverwrite = props.getProperty("destination.overwrite").toBoolean
    val hash = getHash(sourcePath, sourceServer, requester, authorizing)

    SmartContract(
      hash,
      creationTime,
      requester,
      authorizing,
      filename,
      transaction,
      sourceServer,
      sourcePath,
      sourceHeader,
      sourceDelimiter,
      sourceRemove,
      sourceTimeFormat,
      destinationPath,
      destinationDatabase,
      destinationTable,
      destinationFields,
      destinationTypes,
      destinationOverwrite)
  }

  private def validate(sm: SmartContract): Boolean = {
    // TODO create validations
    true
  }

  private def getHash(sourcePath: String, sourceServer:String, requester: String, authorizing: String): String = {
    val bytesTime = Instant.now.toString.getBytes
    val bytes = bytesTime ++ sourcePath.getBytes ++ sourceServer.getBytes ++ requester.getBytes ++ authorizing.getBytes
    val digest = MessageDigest.getInstance("MD5").digest(bytes)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }
}
