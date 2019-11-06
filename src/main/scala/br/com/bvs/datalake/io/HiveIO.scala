package br.com.bvs.datalake.io

import akka.actor.{Actor, ActorLogging, Props, Status}
import br.com.bvs.datalake.core.HivePool

object HiveIO {
  def props(hivePool: HivePool): Props = Props(new HiveIO(hivePool))


}


class HiveIO(hivePool: HivePool) extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = ???
}
