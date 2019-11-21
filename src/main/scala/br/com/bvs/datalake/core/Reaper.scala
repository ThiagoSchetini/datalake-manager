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
      log.error(s"watched actor unexpected terminated: $ref")
      onReap()

    case Reap => onReap()
  }

  private def onReap(): Unit = {
    log.warning("shutting down datalake manager actor system")
    if (watching.nonEmpty)
      watching.foreach(a => context.stop(a))
    actorSystem.terminate()
  }

}
