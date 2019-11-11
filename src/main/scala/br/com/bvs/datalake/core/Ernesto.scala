package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import scala.language.postfixOps
import org.apache.hadoop.fs.FileSystem
import br.com.bvs.datalake.core.Ernesto.{SmartContractKey, SmartContractTick, WatchSmartContractsOn}
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO.ListFilesFrom
import br.com.bvs.datalake.model.CoreMetadata

/**
  * Ernesto the Scheduler
  */
object Ernesto {
  def props(hdfsClient: FileSystem): Props = Props(new Ernesto(hdfsClient))

  case class WatchSmartContractsOn(dir: String)
  private case class SmartContractTick(senderActor: ActorRef, dir: String)
  private case class SmartContractKey(dir: String)
}

class Ernesto(hdfsClient: FileSystem) extends Actor with Timers with ActorLogging {
  private var meta: CoreMetadata = _
  private var hdfsIO: ActorRef = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def preStart(): Unit = {
    meta = PropertiesHelper.getCoreMetadata
    hdfsIO = context.actorOf(HdfsIO.props)
  }

  override def receive: Receive = {
    case WatchSmartContractsOn(dir) =>
      timers.startPeriodicTimer(SmartContractKey(dir), SmartContractTick(sender, dir), meta.smWatchTick.duration)

    case SmartContractTick(senderActor, dir) =>
      hdfsIO.tell(ListFilesFrom(hdfsClient, dir), senderActor)
  }

}