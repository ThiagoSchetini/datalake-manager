package br.com.bvs.datalake.core

import java.sql.Connection
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import br.com.bvs.datalake.core.Reaper.Reap
import org.apache.hadoop.fs.FileSystem
import scala.concurrent.Await
import br.com.bvs.datalake.helper.CorePropertiesHelper
import br.com.bvs.datalake.io.{HdfsPool, HivePool}
import br.com.bvs.datalake.io.HdfsPool.GetHDFSClient
import br.com.bvs.datalake.io.HivePool.GetHiveConnection

object Supervisor {
  def props(reaper: ActorRef): Props = Props(new Supervisor(reaper))
}

class Supervisor(reaper: ActorRef) extends Actor with ActorLogging {
  private var hdfsClientPool: ActorRef = _
  private var hiveConnectionFactory: ActorRef = _
  private var hdfsClient: FileSystem = _
  private var hiveConnection: Connection = _
  private var smRanger: ActorRef = _
  private var ernesto: ActorRef = _

  override def preStart(): Unit = {
    implicit val clientTimeout: Timeout = CorePropertiesHelper.getCoreMetadata.clientTimeout
    hdfsClientPool = context.actorOf(HdfsPool.props)
    hiveConnectionFactory = context.actorOf(HivePool.props)

    val futureHDFSClient = hdfsClientPool ? GetHDFSClient
    try {
      hdfsClient = Await.result(futureHDFSClient, clientTimeout.duration).asInstanceOf[FileSystem]
      log.info("HDFS client created")
    } catch {
      case e: Exception =>
        log.error(s"couldn't create HDFS client: ${e.getMessage}")
        reaper ! Reap
    }

    val futureHiveConnection = hiveConnectionFactory ? GetHiveConnection
    try {
      hiveConnection = Await.result(futureHiveConnection, clientTimeout.duration).asInstanceOf[Connection]

    } catch {
      case e: Exception =>
        log.error(s"couldn't create Hive connection: ${e.getMessage}")
        reaper ! Reap
    }

    ernesto = context.actorOf(Ernesto.props(hdfsClient))
    smRanger = context.actorOf(SmartContractRanger.props(hdfsClient, ernesto))
  }

  override def receive: Receive = {
    case Failure(e) =>
      log.error(e.getMessage)
      reaper ! Reap
  }
}