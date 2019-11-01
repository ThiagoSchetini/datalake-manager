package br.com.bvs.datalake.io

import java.io.{BufferedInputStream, ByteArrayInputStream, File, FileInputStream}
import akka.actor.{Actor, Props, Status}
import org.apache.hadoop.fs._
import HdfsIO._
import br.com.bvs.datalake.helper.HadoopConfigurationHelper

object HdfsIO {
  def props: Props = Props(new HdfsIO)

  case class Upload(hdfs: FileSystem, source: String, target: String)
  case object FileUploaded

  case class Append(name: String, appender: FSDataOutputStream, data: StringBuilder)
  case class Appended(name: String)

  case class ReadFile(hdfs: FileSystem, source: String)
  case class FileDoesNotExist(source: String)
  case class DataFromFile(fileName: String, data: String)

  case class ListFilesFrom(hdfs: FileSystem, source: String)
  case class DirectoryDoesNotExist(source: String)
  case class FilesList(paths: List[Path])
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
      /* warning: do not close the appender, it's done by pool */

    case ReadFile(hdfs, source) =>
      val path = new Path(source)

      if (!hdfs.exists(path) || !hdfs.isFile(path)) {
        sender ! FileDoesNotExist(source)

      } else {
        val inStream = hdfs.open(path)
        //val fileName = source.substring(source.lastIndexOf('/') + 1, source.length)
        val fileName = path.getName
        val bytes = new Array[Byte](bufferSize)
        val data = new StringBuilder

        var numBytes = inStream.read(bytes)
        while (numBytes > 0) {
          data.append(bytes.toString)
          numBytes = inStream.read(bytes)
        }

        inStream.close()
        sender ! DataFromFile(fileName, data.mkString)
      }

    case ListFilesFrom(hdfs, source) =>
      val path = new Path(source)

      if (!hdfs.exists(path) || !hdfs.isDirectory(path)) {
        sender ! DirectoryDoesNotExist(source)
      } else {
        val paths = scala.collection.mutable.ListBuffer[Path]()
        val iterable: RemoteIterator[LocatedFileStatus] = hdfs.listFiles(path, false)

        while (iterable.hasNext) {
          paths += iterable.next.getPath
        }

        sender ! FilesList(paths.toList)
      }
  }
}