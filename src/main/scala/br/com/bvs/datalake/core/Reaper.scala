package br.com.bvs.datalake.core

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import br.com.bvs.datalake.core.Reaper._

object Reaper {
  def props(implicit actorSystem: ActorSystem): Props = Props(new Reaper)
  case class WatchIt(ref: ActorRef)
  object Reap
}

class Reaper(implicit actorSystem: ActorSystem) extends Actor with ActorLogging {
  private val watching = scala.collection.mutable.Set[ActorRef]()

  override def receive: Receive = {
    case WatchIt(actorRef) =>
      context.watch(actorRef)
      watching += actorRef

    case Terminated(ref) =>
      watching -= ref

      if (watching.isEmpty) {
        log.info("all watched actors terminated")
        actorSystem.terminate()
      }

    case Reap =>
      log.warning("forcing actor system to finish")
      actorSystem.terminate()
  }

}
