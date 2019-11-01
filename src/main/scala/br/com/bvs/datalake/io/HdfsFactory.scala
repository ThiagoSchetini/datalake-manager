package br.com.bvs.datalake.io

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import scala.collection.mutable
import br.com.bvs.datalake.helper.HadoopConfigurationHelper
import HdfsFactory._

object HdfsFactory {
  def props: Props = Props(new HdfsFactory)
  case class Appendable(target: String, appender: FSDataOutputStream, hdfsIO: ActorRef)
  case object GetClient
  case class GetAppendable(target: String)
  case class HereGoesTheAppendable(appendable: Appendable)
}

class HdfsFactory extends Actor with ActorLogging {
  private var hdfs: FileSystem = _

  private val appendablePool = mutable.HashMap[String, Appendable]()



  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case GetClient =>
      if (hdfs == null) {
        hdfs = FileSystem.get(HadoopConfigurationHelper.getConfiguration)
      } else {
        /* test the Client and throws Exception if don't connect */
        hdfs.getStatus
      }
      sender ! hdfs

    case GetAppendable(target: String) =>
      appendablePool.get(target) match {
        case Some(appendable) => sender ! HereGoesTheAppendable(appendable)

        case None =>
          val targetPath = new Path(target)

          if (! hdfs.exists(targetPath)) {
            val outStream = hdfs.create(targetPath)
            outStream.close()
          }

          val appender = hdfs.append(targetPath)
          val io = context.actorOf(HdfsIO.props)
          val appendable = Appendable(target, appender, io)
          appendablePool += s"$target" -> appendable
          sender ! HereGoesTheAppendable(appendable)
      }

    case Failure(reason) => sender ! Status.Failure(reason)
  }

  override def postStop(): Unit = {
    stopClient()
  }

  private def stopClient(): Unit = {
    if (appendablePool.nonEmpty) {
      log.info("stopping hdfs client appenders")
      appendablePool.foreach(a => a._2.appender.close())
    }

    if (hdfs != null) {
      log.info("stopping hdfs client")
      hdfs.close()
    }
  }

}
