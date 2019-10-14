package br.com.bvs.datalake

import akka.actor.ActorSystem
import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.core.{Ernesto, Reaper}
import br.com.bvs.datalake.util.TextUtil

object Initializer extends App {

  /* the system */
  implicit val actorSystem: ActorSystem = ActorSystem("datalake-manager")

  /* main actors */
  val reaper = actorSystem.actorOf(Reaper.props, "reaper")

  /* start it all ... */
  TextUtil.printWelcome()

  //import java.nio.file.Paths
  //val path = Paths.get("src/test/mocks").toAbsolutePath.toString

  val ernesto = actorSystem.actorOf(Ernesto.props)
  ernesto ! WatchSmartContractsOn("src/test/mocks")


  //reaper ! WatchIt(...)
  //reaper ! Reap

}
