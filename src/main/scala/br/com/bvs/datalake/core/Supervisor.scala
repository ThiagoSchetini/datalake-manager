package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import br.com.bvs.datalake.core.Ernesto.WatchShutDownSignalOn

import scala.concurrent.Await
import org.apache.hadoop.fs.FileSystem
import br.com.bvs.datalake.core.Reaper.{Reap, WatchIt}
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.core.HdfsPool.GetHDFSClient
import br.com.bvs.datalake.core.Supervisor.ShutDownManager
import br.com.bvs.datalake.io.LocalIO
import br.com.bvs.datalake.io.LocalIO.{FileExists, NotifyIfFileExists, RemoveFile}
import br.com.bvs.datalake.model.meta.CoreMetadata

object Supervisor {
  def props(reaper: ActorRef): Props = Props(new Supervisor(reaper))

  case object ShutDownManager
}

class Supervisor(reaper: ActorRef) extends Actor with ActorLogging {
  private var hdfsClient: FileSystem = _
  private var hdfsPool: ActorRef = _
  private var hivePool: ActorRef = _
  private var smRanger: ActorRef = _
  private var ernesto: ActorRef = _
  private var localIO: ActorRef = _
  private var meta: CoreMetadata = _

  override def preStart(): Unit = {
    implicit val clientTimeout: Timeout = PropertiesHelper.getCoreMetadata.clientTimeout
    hdfsPool = context.actorOf(HdfsPool.props, "hdfs-pool")
    hivePool = context.actorOf(HivePool.props, "hive-pool")

    val futureHDFSClient = hdfsPool ? GetHDFSClient
    try {
      hdfsClient = Await.result(futureHDFSClient, clientTimeout.duration).asInstanceOf[FileSystem]
      log.info("HDFS client created")
    } catch {
      case e: Exception =>
        log.error(s"couldn't create HDFS client: ${e.getMessage}")
        reaper ! Reap
    }

    ernesto = context.actorOf(Ernesto.props(hdfsClient), "ernesto")
    smRanger = context.actorOf(SmartContractRanger.props(hdfsClient, hdfsPool, hivePool, ernesto), "sm-ranger")
    localIO = context.actorOf(LocalIO.props, "local-io")

    meta = PropertiesHelper.getCoreMetadata
    enableTwoWayErrorProtection()
    ernesto ! WatchShutDownSignalOn(s"${meta.shutdownSignalDir}/${meta.shutdownSignalFile}", meta.smWatchTick)
  }

  override def receive: Receive = {
    case Failure(e) =>
      log.error(e.getMessage)
      reaper ! Reap

    case ShutDownManager => reaper ! Reap

    case FileExists(source) =>
      if(source.contains(meta.shutdownSignalFile)) {
        localIO ! RemoveFile(source)
        reaper ! Reap
      }
  }

  private def enableTwoWayErrorProtection(): Unit = {
    reaper ! WatchIt(hdfsPool)
    reaper ! WatchIt(hivePool)
    reaper ! WatchIt(ernesto)
    reaper ! WatchIt(smRanger)
  }
}