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
import br.com.bvs.datalake.core.SmartContractRanger.{TransactionFailed, TransactionSuccess}
import br.com.bvs.datalake.helper._
import br.com.bvs.datalake.io.HiveIO
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseAndTableChecked}
import br.com.bvs.datalake.model.{SmartContract, SubmitMetadata}
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start

object FileToHiveTransaction {
  def props(path: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout):Props =
    Props(new FileToHiveTransaction(path, sm, hivePool, timeout))

  case object Start
}

class FileToHiveTransaction(smPath: Path, sm: SmartContract, hivePool: ActorRef, timeout: Timeout) extends Actor with ActorLogging {
  implicit val clientTimeout: Timeout = timeout
  private var hiveConn: Connection = _
  private var hiveIO: ActorRef = _
  private var cmd: Seq[String] = _
  private var submitMeta: SubmitMetadata = _
  private val pipeline = "CSVToParquet"

  override def preStart(): Unit = {
    val futureHiveConnection = hivePool ? GetHiveConnection
    hiveConn = Await.result(futureHiveConnection, clientTimeout.duration).asInstanceOf[Connection]
    hiveIO = context.actorOf(HiveIO.props)
  }

  override def receive: Receive = {
    case Start =>
      log.info(s"start: ${smPath.getName}")
      val fields = (sm.destinationFields, sm.destinationTypes).zipped.map((_,_)).toList
      hiveIO ! CheckTable(hiveConn, sm.destinationDatabase, sm.destinationTable, sm.destinationPath, fields)

    case DatabaseAndTableChecked =>
      log.info(s"checked: ${sm.destinationDatabase}.${sm.destinationTable}")
      val meta = PropertiesHelper.getSparkMetadata
      var mem, cores, executors, eMem, eCores, connections, retries = 2

      if (meta.production) {
        /* TODO remove this if and production check: send ! YarnResourceCheck (tunning params, create policy for prod and dev)*/
        mem = 24
        cores = 12
        executors = 3
        eMem = 16
        eCores = 12
        connections = 3
        retries = 19
      }

      submitMeta = SubmitMetadata(
        meta.submit,
        meta.mode,
        meta.jar,
        meta.queue,
        mem,
        cores,
        executors,
        eMem,
        eCores,
        connections,
        retries,
        sm.sourcePath,
        sm.destinationPath,
        sm.destinationOverwrite,
        pipeline)

      cmd = SparkHelper.createSparkSubmit(submitMeta)

      val result = sparkSubmit(meta.search, cmd)
      println(result._1)
      println(result._2.mkString)

      if (result._1 == 0)
        context.parent ! TransactionSuccess(smPath)
      else
        context.parent ! TransactionFailed(smPath, result._2.mkString)

    case Failure(e) =>
      context.parent ! TransactionFailed(smPath, e.getMessage)
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
