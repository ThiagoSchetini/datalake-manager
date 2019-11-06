package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.ByteArrayInputStream
import java.util.{Calendar, Properties}
import java.security.MessageDigest
import java.math.BigInteger
import java.time.Instant

import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.helper.CorePropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO._
import br.com.bvs.datalake.model.{CoreMetadata, SmartContract}
import br.com.bvs.datalake.transaction.UploadToHiveTransaction
import br.com.bvs.datalake.transaction.UploadToHiveTransaction.{HiveDataFailed, HiveDataOk, Start}

import scala.collection.mutable

object SmartContractRanger {
  def props(hdfsClient: FileSystem, hivePool: ActorRef, ernesto: ActorRef): Props =
    Props(new SmartContractRanger(hdfsClient, hivePool, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
}

class SmartContractRanger(hdfsClient: FileSystem, hivePool: ActorRef, ernesto: ActorRef) extends Actor with ActorLogging {
  private var ongoingSm: mutable.HashMap[Path, (ActorRef, String)] = _
  private var meta: CoreMetadata = _
  private var hdfsIO: ActorRef = _


  override def preStart(): Unit = {
    ongoingSm = new mutable.HashMap[Path, (ActorRef, String)]()
    meta = CorePropertiesHelper.getCoreMetadata
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")

    meta.smWatchDirs.foreach(dir => {
      hdfsIO ! CheckOrCreateDir(hdfsClient, dir)
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.failDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.ongoingDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.doneDirName}")
      ernesto ! WatchSmartContractsOn(dir)

      /* only first runtime check on ongoing, in case of application break */
      hdfsIO ! ListFilesFrom(hdfsClient, s"$dir/${meta.ongoingDirName}")
    })
  }

  override def receive: Receive = {
    case msg: Failure => context.parent.forward(msg)

    case PathsList(paths) =>
      if (paths.nonEmpty) {
        paths
          .filter(_.getName.contains(meta.smSufix))
          .foreach(p => {
            log.info(s"reading smart contract ${p.getName}")
            hdfsIO ! ReadFile(hdfsClient, p)
          })
      }

    case DataFromFile(path, data) =>
      val props = new Properties()
      // TODO check is this working without toString?
      //props.load(new ByteArrayInputStream(data.toString.getBytes()))
      props.load(new ByteArrayInputStream(data.getBytes()))

      val sm = buildSmartContract(props)

      // TODO validadeSmartContract(sm)
      // TODO if not valid move to failed and do not continue

      // TODO check if ongoing/ongoing is a problem
      if(!path.toString.contains(s"${meta.ongoingDirName}"))
        hdfsIO ! MoveToSubDir(hdfsClient, path, meta.ongoingDirName)

      /* creates unique hash and a new transaction for a Smart Contract here, them start it */
      val hash = hashSM(data.getBytes())
      val transaction = context.actorOf(UploadToHiveTransaction.props(path, sm, hivePool, meta.clientTimeout), s"transaction-$hash")
      ongoingSm += path -> (transaction, serializeSmartContract(path.getName, sm, hash))
      transaction ! Start

    case HiveDataOk(path) =>
      val source = s"path.toString/${meta.ongoingDirName}"
      val target = s"path.toString/${meta.doneDirName}"
      // TODO move sm original file to HDFS on done.dir.name
      // TODO get appender, put the serialized line to HDFSIO file
      // TODO context.stop(transaction)
      // TODO ongoingSm.remove(path)

    case HiveDataFailed(path) =>
      val source = s"path.toString/${meta.ongoingDirName}"
      val target = s"path.toString/${meta.failDirName}"
      // TODO move sm original file to HDFS on fail.dir.name
      // TODO context.stop(transaction)
      // TODO ongoingSm.remove(path)
      // TODO ongoingSm.remove(path)

  }

  private def buildSmartContract(props: Properties): SmartContract = {
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

  private def validadeSmartContract(sm: SmartContract): SmartContract = {
    ???
  }

  private def serializeSmartContract(fileName: String, sm: SmartContract, hash: String): String = {
    val newline = "\n"
    val smBuilder = new StringBuilder()

    smBuilder.append(
      s"""$hash
         |${sm.sourceName}
         |${sm.sourceServer}
         |${sm.sourcePath}
         |${sm.sourceFields}
         |${sm.destinationFields}
         |${sm.destinationTypes}
         |${sm.smartReleasePath}
         |${sm.fileReleasePath}
         |${sm.distributionPaths}
         |${sm.versionPattern}
         |${sm.delimiter}
         |${sm.header}
         |$fileName
         |${Calendar.getInstance.getTime}"""
        .stripMargin.replaceAll(newline, meta.smDelimiter.toString)).append(newline)

    smBuilder.mkString
  }

  private def hashSM(array: Array[Byte]): String = {
    val bytesTime = Instant.now.toString.getBytes
    val sum = array ++ bytesTime
    val digest = MessageDigest.getInstance("MD5").digest(sum)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }

}
