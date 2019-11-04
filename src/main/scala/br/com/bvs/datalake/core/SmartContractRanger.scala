package br.com.bvs.datalake.core

import java.io.ByteArrayInputStream
import java.util.{Calendar, Properties}
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.helper.AppPropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO._
import br.com.bvs.datalake.model.{CoreMetadata, SmartContract}
import br.com.bvs.datalake.transaction.UserHiveDataTransaction
import br.com.bvs.datalake.transaction.UserHiveDataTransaction.{HiveDataFailed, HiveDataOk, Start}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object SmartContractRanger {
  def props(hdfsClient: FileSystem, ernesto: ActorRef): Props = Props(new SmartContractRanger(hdfsClient, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
}

class SmartContractRanger(hdfsClient: FileSystem, ernesto: ActorRef) extends Actor with ActorLogging {
  var ongoingSm: mutable.HashMap[Path, (ActorRef, String)] = _
  var meta: CoreMetadata = _
  var hdfsIO: ActorRef = _

  override def preStart(): Unit = {
    ongoingSm = new mutable.HashMap[Path, (ActorRef, String)]()
    meta = AppPropertiesHelper.getCoreMetadata
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")

    meta.smWatchDirs.foreach(dir => {
      hdfsIO ! CheckOrCreateDir(hdfsClient, dir)
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.failDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.ongoingDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.doneDirName}")
      ernesto ! WatchSmartContractsOn(dir)

      /* only first runtime check on ongoing */
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
      props.load(new ByteArrayInputStream(data.toString.getBytes()))

      val sm = buildSmartContract(props)

      // TODO validadeSmartContract(sm)
      // TODO if not valid move to failed
      // TODO if valid, continue:

      hdfsIO ! MoveToSubDir(hdfsClient, path, meta.ongoingDirName)

      val transaction = context.actorOf(UserHiveDataTransaction.props(path, sm))
      ongoingSm += path -> (transaction, serializeSmartContract(path.getName, sm))
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

  private def serializeSmartContract(fileName: String, sm: SmartContract): String = {
    val newline = "\n"
    val smBuilder = new StringBuilder()

    smBuilder.append(
      s"""${sm.sourceName}
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

}
