package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import scala.language.postfixOps
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import br.com.bvs.datalake.core.Ernesto.{SmartContractKey, SmartContractTick, WatchSmartContractsOn}
import br.com.bvs.datalake.helper.AppPropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO.ListFilesFrom
import br.com.bvs.datalake.model.CoreMetadata
import org.apache.hadoop.fs.FileSystem

/**
  * Ernesto the Scheduler
  */
object Ernesto {
  def props(hdfsClient: FileSystem): Props = Props(new Ernesto(hdfsClient))

  case class WatchSmartContractsOn(dir: String)
  private case class SmartContractTick(senderActor: ActorRef, dir: String)
  private case object SmartContractKey
}

class Ernesto(hdfsClient: FileSystem) extends Actor with Timers with ActorLogging {
  var meta: CoreMetadata = _
  var hdfsIO: ActorRef = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def preStart(): Unit = {
    meta = AppPropertiesHelper.getCoreMetadata
    hdfsIO = context.actorOf(HdfsIO.props)
  }

  override def receive: Receive = {
    case WatchSmartContractsOn(dir) =>
      timers.startPeriodicTimer(SmartContractKey, SmartContractTick(sender, dir), meta.smWatchTick.duration)

    case SmartContractTick(senderActor, dir) =>
      hdfsIO ! (ListFilesFrom(hdfsClient, dir), senderActor)
  }

}