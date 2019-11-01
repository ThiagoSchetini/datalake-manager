package br.com.bvs.datalake.core

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.Status.Failure

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import br.com.bvs.datalake.sm.SmartContractRanger.ReadSmartContract
import br.com.bvs.datalake.core.Ernesto.{SmartContractKey, SmartContractTick, WatchSmartContractsOn}
import br.com.bvs.datalake.core.Reaper.Reap
import br.com.bvs.datalake.exception.ErnestoException
import br.com.bvs.datalake.io.HdfsIO.FilesList
import org.apache.hadoop.fs.FileSystem

/**
  * Ernesto the Scheduler
  */
object Ernesto {
  def props(hdfsClient: FileSystem): Props = Props(new Ernesto(hdfsClient))

  case class WatchSmartContractsOn(directory: String)
  private case class SmartContractTick(senderActor: ActorRef, directory: String)
  private case object SmartContractKey

  // TODO next protocols
}

class Ernesto(hdfsClient: FileSystem) extends Actor with Timers with ActorLogging {

  val failSubName = "fail"
  val ongoingSubName = "ongoing"
  val smartContractSufix = "properties"

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {
    case WatchSmartContractsOn(directory) =>
      //mkdir
      //mk faildir
      //mk ongoing
      timers.startPeriodicTimer(SmartContractKey, SmartContractTick(sender, directory), 3 seconds)

    case SmartContractTick(senderActor, directory) =>
      // tell HDFS IO List files on dir
      ???

    case FilesList(files) =>
      self.forward(smRanger)





  }

}