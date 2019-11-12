package br.com.bvs.datalake

import akka.actor.ActorSystem
import br.com.bvs.datalake.core.Reaper.WatchIt
import br.com.bvs.datalake.core.{Reaper, Supervisor}
import br.com.bvs.datalake.util.TextUtil

object Initializer extends App {

  /* start it all ... */
  TextUtil.printWelcome()

  /* registration required for legacy Hive/Driver */
  Class.forName("org.apache.hive.jdbc.HiveDriver")

  /* the system */
  implicit val actorSystem: ActorSystem = ActorSystem("datalake-manager")

  /* main actors */
  val reaper = actorSystem.actorOf(Reaper.props, "reaper")
  val supervisor = actorSystem.actorOf(Supervisor.props(reaper), "supervisor")

  reaper ! WatchIt(supervisor)
}
