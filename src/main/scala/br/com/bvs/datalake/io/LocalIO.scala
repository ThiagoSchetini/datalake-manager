package br.com.bvs.datalake.io

import java.nio.file.{Files, Paths}
import akka.Done
import akka.actor.{Actor, Props, Status}
import scala.xml.{Elem, XML}
import LocalIO._

object LocalIO {
  def props: Props = Props(new LocalIO)
  case class CreateElemFrom(source: String)
  case class ElemCreated(elem: Elem)
  case class RemoveFile(source: String)
  case class MoveFile(source: String, target: String)
}

class LocalIO extends Actor {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case CreateElemFrom(source) =>
      val elem = XML.loadFile(source)
      sender ! ElemCreated(elem)

    case RemoveFile(source) =>
      Files.delete(Paths.get(source))
      sender ! Done

    case MoveFile(source, target) =>
      Files.deleteIfExists(Paths.get(target))
      Files.move(Paths.get(source), Paths.get(target))
      sender ! Done
  }

}