package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.Await
import org.apache.hadoop.fs.Path
import java.sql.Connection

import br.com.bvs.datalake.core.HivePool.GetHiveConnection
import br.com.bvs.datalake.io.HiveIO
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseAndTableChecked}
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.transaction.UploadToHiveTransaction.Start

object UploadToHiveTransaction {
  def props(path: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout):Props =
    Props(new UploadToHiveTransaction(path, sm, hivePool, timeout))

  case object Start
  case class HiveDataOk(path: Path)
  case class HiveDataFailed(path: Path)
}

class UploadToHiveTransaction(path: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout) extends Actor with ActorLogging {
  implicit val clientTimeout: Timeout = timeout
  private var hiveConn: Connection = _
  private var hiveIO: ActorRef = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def preStart(): Unit = {
    val futureHiveConnection = hivePool ? GetHiveConnection
    hiveConn = Await.result(futureHiveConnection, clientTimeout.duration).asInstanceOf[Connection]
    hiveIO = context.actorOf(HiveIO.props)
  }

  override def receive: Receive = {
    case Start => {
      log.info(s"start: ${path.getName}")
      hiveIO ! CheckTable(hiveConn, sm.database, sm.table)
    }

    case DatabaseAndTableChecked =>
      log.info(s"checked: ${sm.database}.${sm.table}")



    case Failure(e) => {
      println(e.getMessage)
      log.info(s"Transaction failed for: ${path.getName}")
    }

  }
}
