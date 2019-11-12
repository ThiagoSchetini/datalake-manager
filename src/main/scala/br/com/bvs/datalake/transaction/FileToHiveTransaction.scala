package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.Await
import scala.sys.process._
import org.apache.hadoop.fs.Path
import java.sql.Connection

import br.com.bvs.datalake.core.HivePool.GetHiveConnection
import br.com.bvs.datalake.helper._
import br.com.bvs.datalake.io.HiveIO
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseAndTableChecked}
import br.com.bvs.datalake.model.{Developer, Production, SmartContract, SubmitMetadata}
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
  private var cmd: Seq[String] = _
  private var submitMeta: SubmitMetadata = _
  private val sparkFlow = "CSVToParquet"

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

      /* start TODO -> send ! YarnResourceCheck (tunning params) */
      if (meta.production) {
        val mem = 24
        val cores = 12
        val executors = 3
        val eMem = 16
        val eCores = 12
        val connections = 3
        val retries = 19
        submitMeta = SubmitMetadata(meta.submit, meta.mode, meta.jar, meta.queue, mem, cores, executors, eMem, eCores, connections, retries)
      } else {
        val mem = 2
        val cores = 2
        val executors = 2
        val eMem = 2
        val eCores = 2
        val connections = 2
        val retries = 10
        submitMeta = SubmitMetadata(meta.submit, meta.mode, meta.jar, meta.queue, mem, cores, executors, eMem, eCores, connections, retries)
      }
      /* end TODO */

      cmd = SparkHelper.createSubmit(submitMeta)

      // TODO add to submit the spark method and paths
      val result = sparkSubmit(meta.search, cmd)
      println(result._1)
      println(result._2.mkString)

    case Failure(e) =>
      log.info(s"Transaction failed for ${path.getName}: ${e.getMessage}")
  }

  private def sparkSubmit(search: String, cmd: Seq[String]): (Int, StringBuilder) = {
    val builder = new StringBuilder()

    val result = cmd ! ProcessLogger(log => {
      if(log.contains(search))
        builder.append(s"$log\n")
    })

    (result, builder)
  }
}
