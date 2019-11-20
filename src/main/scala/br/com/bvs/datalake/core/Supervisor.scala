package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.Await
import org.apache.hadoop.fs.FileSystem
import br.com.bvs.datalake.core.Reaper.Reap
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.core.HdfsPool.GetHDFSClient

object Supervisor {
  def props(reaper: ActorRef): Props = Props(new Supervisor(reaper))
}

class Supervisor(reaper: ActorRef) extends Actor with ActorLogging {
  private var hdfsPool: ActorRef = _
  private var hdfsClient: FileSystem = _
  private var hivePool: ActorRef = _
  private var smRanger: ActorRef = _
  private var ernesto: ActorRef = _

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
  }

  override def receive: Receive = {
    case Failure(e) =>
      log.error(e.getMessage)
      reaper ! Reap
  }
}