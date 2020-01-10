package br.com.bvs.datalake.io

import java.io.{BufferedInputStream, BufferedReader, ByteArrayInputStream, File, FileInputStream, InputStreamReader}
import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Status.Failure
import org.apache.hadoop.fs._
import HdfsIO._
import br.com.bvs.datalake.helper.HadoopConfHelper

object HdfsIO {
  def props: Props = Props(new HdfsIO)

  case class Upload(hdfsClient: FileSystem, source: String, target: String)
  case object FileUploaded

  case class Append(name: String, appender: FSDataOutputStream, data: StringBuilder)
  case class Appended(name: String)

  case class ReadFile(hdfsClient: FileSystem, path: Path)
  case class FileDoesNotExist(source: String)
  case class DataFromFile(path: Path, data: String)

  case class OverwriteFileWithData(hdfsClient: FileSystem, targetPath: Path, data: String)

  case class ListFilesFrom(hdfsClient: FileSystem, source: String)
  case class DirectoryDoesNotExist(source: String)
  case class PathsList(paths: List[Path])

  case class CheckOrCreateDir(hdfsClient: FileSystem, dir: String)

  case class MoveTo(hdfsClient: FileSystem, sourcePath: Path, targetPath: Path)

  case class RemoveDirectory(hdfsClient: FileSystem, path: Path)
  case class RemoveFile(hdfsClient: FileSystem, path: Path)
}

class HdfsIO extends Actor with ActorLogging {
  private var bufferSize: Int = _

  override def preStart(): Unit = {
    bufferSize = HadoopConfHelper.getIOFileBufferSize
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Failure(reason)
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
      val inStream = new ByteArrayInputStream(data.toString.getBytes())
      val inBuffer = new BufferedInputStream(inStream)
      val bytes = new Array[Byte](bufferSize)

      var numBytes = inBuffer.read(bytes)
      while (numBytes > 0) {
        appender.write(bytes, 0, numBytes)
        numBytes = inBuffer.read(bytes)
      }

      inBuffer.close()
      inStream.close()
      sender ! Appended(target)
      /* warning: do not close the appender, it's done by pool */

    case ReadFile(hdfsClient, path) =>
      if (!hdfsClient.exists(path)) {
        sender ! FileDoesNotExist(path.toString)

      } else {
        val inStream = hdfsClient.open(path)
        val inBuffer = new BufferedReader(new InputStreamReader(inStream))
        val data = new StringBuilder

        var line: String = inBuffer.readLine
        while (line != null) {
          data.append(line).append("\n")
          line = inBuffer.readLine
        }

        inBuffer.close()
        inStream.close()
        sender ! DataFromFile(path, data.mkString)
      }

    case OverwriteFileWithData(hdfsClient, targetPath, data) =>
      val inBuffer = new ByteArrayInputStream(data.toString.getBytes())
      val outBuffer = hdfsClient.create(targetPath, true)
      val bytes = new Array[Byte](bufferSize)

      var numBytes = inBuffer.read(bytes)
      while (numBytes > 0) {
        outBuffer.write(bytes, 0, numBytes)
        numBytes = inBuffer.read(bytes)
      }

      inBuffer.close()
      outBuffer.close()

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

    case MoveTo(hdfsClient, sourcePath, targetPath) =>
      if(hdfsClient.exists(targetPath))
        hdfsClient.delete(targetPath, false)

      val result = hdfsClient.rename(sourcePath, targetPath)

      if (result)
        log.info(s"$sourcePath moved to $targetPath")

    case RemoveDirectory(hdfsClient, path) =>
      /*
        warning 1: hdfsClient.isFile doesn't work: returns false even when it is a file

        warning 2: when is file, it removes everything inside the directory,
        but when is directory, it removes the entire directory

        solution: always remove the directory
       */
      if (hdfsClient.exists(path)) {
        var directory = path

        if (isFile(path))
          directory = path.getParent

        val result = hdfsClient.delete(directory, true)

        if (result)
          log.info(s"removed directory $directory ")
      }

    case RemoveFile(hdfsClient, path) =>
      hdfsClient.delete(path, false)
  }

  private def isFile(path: Path): Boolean = {
    val last = path.getName
    last.contains(".")
  }
}