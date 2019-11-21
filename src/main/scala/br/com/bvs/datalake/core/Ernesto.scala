package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.util.Timeout
import scala.language.postfixOps
import org.apache.hadoop.fs.FileSystem
import br.com.bvs.datalake.core.Ernesto.{ShutDownTick, ShutDownUniqueKey, SmartContractKey, SmartContractTick, WatchShutDownSignalOn, WatchSmartContractsOn}
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO.ListFilesFrom
import br.com.bvs.datalake.io.LocalIO
import br.com.bvs.datalake.io.LocalIO.NotifyIfFileExists

/**
  * Ernesto the Scheduler
  */
object Ernesto {
  def props(hdfsClient: FileSystem): Props = Props(new Ernesto(hdfsClient))

  case class WatchSmartContractsOn(dir: String, tickTime: Timeout)
  private case class SmartContractTick(senderActor: ActorRef, dir: String)
  private case class SmartContractKey(dir: String)

  case class WatchShutDownSignalOn(dir: String, tickTime: Timeout)
  private case class ShutDownTick(senderActor: ActorRef, dir: String)
  private case object ShutDownUniqueKey
}

class Ernesto(hdfsClient: FileSystem) extends Actor with Timers with ActorLogging {
  private var hdfsIO: ActorRef = _
  private var localIO: ActorRef = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def preStart(): Unit = {
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")
    localIO = context.actorOf(LocalIO.props, "local-io")
  }

  override def receive: Receive = {
    case WatchSmartContractsOn(dir, tickTime) =>
      timers.startPeriodicTimer(SmartContractKey(dir), SmartContractTick(sender, dir), tickTime.duration)

    case SmartContractTick(senderActor, dir) =>
      hdfsIO.tell(ListFilesFrom(hdfsClient, dir), senderActor)

    case WatchShutDownSignalOn(dir, tickTime) =>
      timers.startPeriodicTimer(ShutDownUniqueKey, ShutDownTick(sender, dir), tickTime.duration)

    case ShutDownTick(senderActor, dir) =>
      localIO.tell(NotifyIfFileExists(dir), senderActor)

    case Failure(e) => context.parent ! Failure(e)
  }

}