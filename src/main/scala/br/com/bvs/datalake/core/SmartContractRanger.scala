package br.com.bvs.datalake.core

import java.util.Properties
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.helper.AppPropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO.{CheckOrCreateDir, DataFromFile, PathsList, ReadFile}
import br.com.bvs.datalake.model.{CoreMetadata, SmartContract}
import org.apache.hadoop.fs.FileSystem

object SmartContractRanger {
  def props(hdfsClient: FileSystem, ernesto: ActorRef): Props = Props(new SmartContractRanger(hdfsClient, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
}

class SmartContractRanger(hdfsClient: FileSystem, ernesto: ActorRef) extends Actor with ActorLogging {
  var meta: CoreMetadata = _
  var hdfsIO: ActorRef = _

  override def preStart(): Unit = {
    meta = AppPropertiesHelper.getCoreMetadata
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")

    meta.smWatchDirs.foreach(dir => {
      hdfsIO ! CheckOrCreateDir(hdfsClient, dir)
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.failDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.ongoingDirName}")
      ernesto ! WatchSmartContractsOn(dir)
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

    case DataFromFile(fileName, data) =>
      println(fileName)
      println(data)
      println(data.toString)
      // TODO move sm to ongoing
      // TODO buildSmartContract()
      // TODO validadeSmartContract()
      // TODO serializeSmartContract()
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

  private def serializeSmartContract(sm: SmartContract): String = {
    ???
  }

}
