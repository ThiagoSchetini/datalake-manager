package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.Await
import org.apache.hadoop.fs.Path
import java.sql.Connection
import br.com.bvs.datalake.core.HivePool.GetHiveConnection
import br.com.bvs.datalake.helper._
import br.com.bvs.datalake.io.HiveIO
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseAndTableChecked}
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start

object FileToHiveTransaction {
  def props(path: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout):Props =
    Props(new FileToHiveTransaction(path, sm, hivePool, timeout))

  case object Start
  case class HiveDataOk(path: Path)
  case class HiveDataFailed(path: Path)
}

class FileToHiveTransaction(path: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout) extends Actor with ActorLogging {
  implicit val clientTimeout: Timeout = timeout
  private var hiveConn: Connection = _
  private var hiveIO: ActorRef = _
  private var submit: StringBuilder = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def preStart(): Unit = {
    val futureHiveConnection = hivePool ? GetHiveConnection
    hiveConn = Await.result(futureHiveConnection, clientTimeout.duration).asInstanceOf[Connection]
    hiveIO = context.actorOf(HiveIO.props)
  }

  override def receive: Receive = {
    case Start =>
      log.info(s"start: ${path.getName}")
      val fields = (sm.destinationFields, sm.destinationTypes).zipped.map((_,_))
      hiveIO ! CheckTable(hiveConn, sm.destinationDatabase, sm.destinationTable, sm.destinationPath, fields)

    case DatabaseAndTableChecked =>
      log.info(s"checked: ${sm.destinationDatabase}.${sm.destinationTable}")
      val meta = PropertiesHelper.getSparkMetadata

      if (meta.production) {
        /* TODO dynamic tuning params by job */
        val mem = 24
        val cores = 12
        val executors = 3
        val eMem = 16
        val eCores = 12
        val connections = 3
        val retries = 19

        val sparkJob = SparkJobProduction(meta.jar, meta.queue, mem, cores, executors, eMem, eCores, connections, retries)
        submit = SparkJobHelper.serializeSubmit(sparkJob)

      } else
        submit = SparkJobHelper.serializeSubmit(SparkJobDeveloper(meta.jar))

      // TODO add to submit the spark method and paths
      println(submit.mkString)

      // TODO monitor the shell submit

    case Failure(e) =>
      log.info(s"Transaction failed for ${path.getName}: ${e.getMessage}")
  }
}
