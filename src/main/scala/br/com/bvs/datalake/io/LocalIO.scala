package br.com.bvs.datalake.io

import akka.Done
import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import java.nio.file.{Files, Paths}
import br.com.bvs.datalake.io.LocalIO.{FileExists, MoveFile, NotifyIfFileExists, RemoveFile}

object LocalIO {
  def props: Props = Props(new LocalIO)

  case class NotifyIfFileExists(source: String)
  case class FileExists(source: String)
  case class RemoveFile(source: String)
  case class MoveFile(source: String, target: String)
}

class LocalIO extends Actor {
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Failure(reason)
  }

  override def receive: Receive = {
    case NotifyIfFileExists(source) =>
      if(Files.exists(Paths.get(source)))
        sender ! FileExists(source)

    case RemoveFile(source) =>
      Files.delete(Paths.get(source))
      sender ! Done

    case MoveFile(source, target) =>
      Files.deleteIfExists(Paths.get(target))
      Files.move(Paths.get(source), Paths.get(target))
      sender ! Done
  }
}
