package br.com.bvs.datalake.io

import java.io._
import akka.actor.{Actor, ActorRef, Props, Status}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import HdfsIO._
import br.com.bvs.datalake.helper.HadoopConfigurationHelper

object HdfsIO {
  def props: Props = Props(new HdfsIO)
  case class Upload(hdfs: FileSystem, source: String, target: String)
  case object FileUploaded
  case class Append(name: String, appender: FSDataOutputStream, data: StringBuilder)
  case class Appended(name: String)
  case class CreateAppender(hdfs: FileSystem, target: String, requester: ActorRef)
  case class AppenderCreated(target: String, appender: FSDataOutputStream, requester: ActorRef)
}

class HdfsIO extends Actor {
  private var bufferSize: Int = _

  override def preStart(): Unit = {
    bufferSize = HadoopConfigurationHelper.getIOFileBufferSize
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case Upload(hdfs, source, target) =>
      val inStream = new FileInputStream(new File(source))
      val inBuffer = new BufferedInputStream(inStream)
      val outStream = hdfs.create(new Path(target))
      val bytes = new Array[Byte](bufferSize)
      var numBytes = inBuffer.read(bytes)

      while (numBytes > 0) {
        outStream.write(bytes, 0, numBytes)
        numBytes = inBuffer.read(bytes)
      }

      inBuffer.close()
      inStream.close()
      outStream.close()
      sender ! FileUploaded

    case Append(target, appender, data) =>
      val inBuffer = new BufferedInputStream(new ByteArrayInputStream(data.toString().getBytes()))
      val bytes = new Array[Byte](bufferSize)
      var numBytes = inBuffer.read(bytes)

      while (numBytes > 0) {
        appender.write(bytes, 0, numBytes)
        numBytes = inBuffer.read(bytes)
      }

      inBuffer.close()
      sender ! Appended(target)

  }

}