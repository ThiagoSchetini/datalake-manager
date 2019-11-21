package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import scala.collection.mutable
import br.com.bvs.datalake.core.HdfsPool._
import br.com.bvs.datalake.helper.HadoopConfigurationHelper
import br.com.bvs.datalake.io.HdfsIO

object HdfsPool {
  def props: Props = Props(new HdfsPool)
  case class Appendable(target: String, appender: FSDataOutputStream, hdfsIO: ActorRef)
  case object GetHDFSClient
  case class GetAppendable(target: String)
}

class HdfsPool extends Actor with ActorLogging {

  /* one unique client from this pool */
  private var hdfsClient: FileSystem = _

  /* one unique appender per file from this pool */
  private val appendablePool = mutable.HashMap[String, Appendable]()

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {
    case GetHDFSClient =>
      if (hdfsClient == null) {
        hdfsClient = FileSystem.get(HadoopConfigurationHelper.getConfiguration)
      } else {
        /* test the Client and throws Exception if don't connect */
        hdfsClient.getStatus
      }
      sender ! hdfsClient

    case GetAppendable(target: String) =>
      appendablePool.get(target) match {
        case Some(appendable) => sender ! appendable

        case None =>
          val targetPath = new Path(target)

          if (! hdfsClient.exists(targetPath)) {
            val outStream = hdfsClient.create(targetPath)
            outStream.close()
          }

          val appender = hdfsClient.append(targetPath)
          val io = context.actorOf(HdfsIO.props)
          val appendable = Appendable(target, appender, io)
          appendablePool += s"$target" -> appendable
          sender ! appendable
      }
  }

  override def postStop(): Unit = {
    stopClient()
  }

  private def stopClient(): Unit = {
    if (appendablePool.nonEmpty) {
      log.info("stopping HDFS client open appenders")
      appendablePool.foreach(a => a._2.appender.close())
    }

    if (hdfsClient != null) {
      log.info("stopping HDFS client")
      hdfsClient.close()
    }
  }

}
