package br.com.bvs.datalake

import akka.actor.ActorSystem
import br.com.bvs.datalake.core.Reaper.WatchIt
import br.com.bvs.datalake.core.{Reaper, Supervisor}
import br.com.bvs.datalake.util.TextUtil

object Initializer extends App {

  /* start it all ... */
  TextUtil.printWelcome()

  /* the system */
  implicit val actorSystem: ActorSystem = ActorSystem("datalake-manager")

  /* main actors */
  val reaper = actorSystem.actorOf(Reaper.props, "reaper")
  val supervisor = actorSystem.actorOf(Supervisor.props, "supervisor")

  reaper ! WatchIt(supervisor)
}
