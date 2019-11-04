package br.com.bvs.datalake.io

import java.io.{BufferedInputStream, ByteArrayInputStream, File, FileInputStream}

import akka.actor.{Actor, ActorLogging, Props, Status}
import org.apache.hadoop.fs._
import HdfsIO._
import akka.Done
import br.com.bvs.datalake.helper.HadoopConfigurationHelper

object HdfsIO {
  def props: Props = Props(new HdfsIO)

  case class Upload(hdfsClient: FileSystem, source: String, target: String)
  case object FileUploaded

  case class Append(name: String, appender: FSDataOutputStream, data: StringBuilder)
  case class Appended(name: String)

  case class ReadFile(hdfsClient: FileSystem, path: Path)
  case class FileDoesNotExist(source: String)
  case class DataFromFile(fileName: String, data: String)

  case class ListFilesFrom(hdfsClient: FileSystem, source: String)
  case class DirectoryDoesNotExist(source: String)
  case class PathsList(paths: List[Path])

  case class CheckOrCreateDir(hdfsClient: FileSystem, dir: String)
}

class HdfsIO extends Actor with ActorLogging {
  private var bufferSize: Int = _

  override def preStart(): Unit = {
    bufferSize = HadoopConfigurationHelper.getIOFileBufferSize
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case Upload(hdfsClient, source, target) =>
      val inStream = new FileInputStream(new File(source))
      val inBuffer = new BufferedInputStream(inStream)
      val outStream = hdfsClient.create(new Path(target))
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

    case ReadFile(hdfsClient, path) =>
      if (!hdfsClient.exists(path) || !hdfsClient.isFile(path)) {
        sender ! FileDoesNotExist(source)

      } else {
        val inStream = hdfsClient.open(path)
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

    case ListFilesFrom(hdfsClient, source) =>
      val path = new Path(source)

      if (!hdfsClient.exists(path) || !hdfsClient.isDirectory(path)) {
        sender ! DirectoryDoesNotExist(source)
      } else {
        val paths = scala.collection.mutable.ListBuffer[Path]()
        val iterable: RemoteIterator[LocatedFileStatus] = hdfsClient.listFiles(path, false)

        while (iterable.hasNext) {
          paths += iterable.next.getPath
        }

        sender ! PathsList(paths.toList)
      }

    case CheckOrCreateDir(hdfsClient, dir) =>
      val result = hdfsClient.mkdirs(new Path(dir))
      log.info(s"$dir checked: $result")
      //if(result)
        //sender ! Done
  }
}