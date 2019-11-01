package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.Await
import br.com.bvs.datalake.helper.AppPropertiesHelper
import br.com.bvs.datalake.io.HdfsPool
import br.com.bvs.datalake.io.HdfsPool.GetClient
import br.com.bvs.datalake.model.SupervisorMetadata

object Supervisor {
  def props: Props = Props(new Supervisor)
}

class Supervisor extends Actor with ActorLogging {
  private var meta: SupervisorMetadata = _
  private var hdfsPool: ActorRef = _
  private var hdfsClient: FileSystem = _
  private var smRanger: ActorRef = _
  private var ernesto: ActorRef = _

  override def preStart(): Unit = {
    meta = AppPropertiesHelper.getSupervisorMetadata
    hdfsPool = context.actorOf(HdfsPool.props)

    implicit val timeout: Timeout = meta.hdfsClientTimeout
    val futureClient = hdfsPool ? GetClient

    try {
      hdfsClient = Await.result(futureClient, meta.hdfsClientTimeout.duration).asInstanceOf[FileSystem]
      log.info("HDFS client created")
    } catch {
      case e: Exception =>
        log.error(s"couldn't create HDFS client: ${e.getMessage}")
        context.stop(self)
    }

    ernesto = context.actorOf(Ernesto.props(hdfsClient))
    smRanger = context.actorOf(SmartContractRanger.props(hdfsClient, ernesto))
  }

  override def receive: Receive = {
    case Failure => context.stop(self)
  }
}
