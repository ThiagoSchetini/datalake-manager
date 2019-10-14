package br.com.bvs.datalake.core

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import br.com.bvs.datalake.contract.SmartContractRanger
import br.com.bvs.datalake.contract.SmartContractRanger.ReadSmartContract
import br.com.bvs.datalake.core.Ernesto.{SmartContractKey, SmartContractTick, WatchSmartContractsOn}
import br.com.bvs.datalake.exception.ErnestoException

/**
  * Ernesto the "scheduler"
  */
object Ernesto {
  def props: Props = Props(new Ernesto)

  private case class SmartContractTick(directory: String)
  private case object SmartContractKey
  case class WatchSmartContractsOn(directory: String)
}

class Ernesto extends Actor with Timers with ActorLogging {
  var smartContractRanger: ActorRef = _
  val failSubName = "fail"
  val ongoingSubName = "ongoing"
  val smartSufix = "properties"

  override def preStart(): Unit = {
    smartContractRanger = context.actorOf(SmartContractRanger.props)
  }

  override def receive: Receive = {
    case WatchSmartContractsOn(directory) =>
      checkDirectory(directory)
      checkSubDirectory(directory, failSubName)
      checkSubDirectory(directory, ongoingSubName)
      timers.startPeriodicTimer(SmartContractKey, SmartContractTick(directory), 3 seconds)

    case SmartContractTick(directory) =>
      val smarts = new File(directory)
        .listFiles()
        .filter(_.isFile)
        .filter(_.getName.endsWith(smartSufix))
        .map(_.getName)
        .toSet

      smarts.foreach(f => {
        fileSecureMove(s"$directory/$f", s"$directory/$ongoingSubName/$f")
        smartContractRanger ! ReadSmartContract(s"$directory/$ongoingSubName", f)
      })
  }

  private def checkDirectory(directory: String): Unit = {
    val d = new File(directory)
    if (! d.isDirectory)
      throw new ErnestoException(s"$d is not a directory")
  }

  private def checkSubDirectory(directory: String, subName: String): String = {
    val sub = Paths.get(s"$directory/$subName")
    if (Files.notExists(sub))
      Files.createDirectory(sub)

    s"$directory/$subName"
  }

  private def fileSecureMove(source: String, target: String): Unit = {
    Files.deleteIfExists(Paths.get(target))
    Files.move(Paths.get(source), Paths.get(target))
  }
}