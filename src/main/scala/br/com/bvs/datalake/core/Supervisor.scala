package br.com.bvs.datalake.core

import java.sql.Connection

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import br.com.bvs.datalake.core.Reaper.Reap
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.Await
import br.com.bvs.datalake.helper.AppPropertiesHelper
import br.com.bvs.datalake.io.{HdfsClientPool, HiveConnectionFactory}
import br.com.bvs.datalake.io.HdfsClientPool.GetHDFSClient
import br.com.bvs.datalake.io.HiveConnectionFactory.GetHiveConnection

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
    hdfsClientPool = context.actorOf(HdfsClientPool.props)
    hiveConnectionFactory = context.actorOf(HiveConnectionFactory.props)

    implicit val timeout: Timeout = AppPropertiesHelper.getCoreMetadata.hdfsClientTimeout

    val futureHDFSClient = hdfsClientPool ? GetHDFSClient
    try {
      hdfsClient = Await.result(futureHDFSClient, timeout.duration).asInstanceOf[FileSystem]
      log.info("HDFS client created")
    } catch {
      case e: Exception =>
        log.error(s"couldn't create HDFS client: ${e.getMessage}")
        reaper ! Reap
    }

    val futureHiveConnection = hiveConnectionFactory ? GetHiveConnection
    try {
      hiveConnection = Await.result(futureHiveConnection, timeout.duration).asInstanceOf[Connection]
      log.info("Hive connection created")
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