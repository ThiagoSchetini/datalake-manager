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

  import scala.sys.process._
  val search = "Exception"
  val submitCmd = "spark-submit"
  val modeFlag = "--deploy-mode"
  val mode = "cluster"
  val retriesFlag = "--conf"
  val retries = "spark.yarn.maxAppAttempts=1"
  val jar = "datalake-spark-0.1-jar-with-dependencies.jar"

  def sparkSubmit(search: String, cmd: String, modeFlag: String, mode: String, retriesFlag: String, retries: String, jar: String): (Int, StringBuilder) = {
    val builder = new StringBuilder()

    val submitCmd = Seq(cmd, modeFlag, mode, retriesFlag, retries, jar)
    val result = submitCmd ! ProcessLogger(log => {
      if(log.contains(search))
        builder.append(s"$log\n")
    })

    (result, builder)
  }

  val result = sparkSubmit(search, submitCmd, modeFlag, mode, retriesFlag, retries, jar)
  println("##################### PRINTLN RES #######################")
  println(result._1)
  println(result._2.mkString)
}
