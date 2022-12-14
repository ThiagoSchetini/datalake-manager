package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.Await
import scala.sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.sql.Connection

import br.com.bvs.datalake.builder.SparkSubmitBuilder
import br.com.bvs.datalake.core.HivePool.{DisposeConnection, GetHiveConnection}
import br.com.bvs.datalake.core.SmartContractRanger.{TransactionFailed, TransactionSuccess}
import br.com.bvs.datalake.helper._
import br.com.bvs.datalake.io.HdfsIO.RemoveDirectory
import br.com.bvs.datalake.io.{HdfsIO, HiveIO}
import br.com.bvs.datalake.io.HiveIO.{CheckTable, TableChecked}
import br.com.bvs.datalake.model.meta.SubmitMetadata
import br.com.bvs.datalake.model.property.FileToHiveProps
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start
import br.com.bvs.datalake.util.TextUtil

object FileToHiveTransaction {
  def props(path: Path, props: FileToHiveProps, hdfsClient: FileSystem, hivePool: ActorRef, timeout: Timeout):Props =
    Props(new FileToHiveTransaction(path, props, hdfsClient, hivePool, timeout))

  case object Start
}

class FileToHiveTransaction(path: Path, props: FileToHiveProps, hdfsClient: FileSystem, hivePool: ActorRef, timeout: Timeout)
  extends Actor with ActorLogging {
  private var hiveConn: Connection = _
  private var hiveIO: ActorRef = _
  private var hdfsIO: ActorRef = _
  private var cmd: Seq[String] = _
  private var submitMeta: SubmitMetadata = _
  private val pipeline = "CSVToParquet"

  override def preStart(): Unit = {
    implicit val clientTimeout: Timeout = timeout
    val futureHiveConnection = hivePool ? GetHiveConnection
    try {
      hiveConn = Await.result(futureHiveConnection, clientTimeout.duration).asInstanceOf[Connection]
      log.info("got Hive connection")
    } catch {
      case e: Exception =>
        log.error(s"couldn't get Hive connection")
        context.parent ! Failure(e)
    }
    hiveIO = context.actorOf(HiveIO.props, "hive-io")
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")
  }

  override def receive: Receive = {
    case Start =>
      log.info(s"start: ${path.getName}")
      val fields = (props.destinationFields, props.destinationTypes).zipped.map((_,_)).toList
      hiveIO ! CheckTable(hiveConn, props.destinationDatabase, props.destinationTable, props.destinationPath, fields)

    case TableChecked =>
      log.info(s"checked: ${props.destinationDatabase}.${props.destinationTable}")
      val meta = PropertiesHelper.getSparkMetadata
      var mem, cores, executors, eMem, eCores, connections, retries = 2

      if (meta.production) {
        /* TODO remove this if and production check: send ! YarnResourceCheck (tunning params, create policy for prod and dev)*/
        mem = 24
        cores = 12
        executors = 2
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
        props.sourcePath,
        props.destinationPath,
        props.destinationOverwrite,
        pipeline,
        TextUtil.serializeList(props.destinationTypes),
        props.sourceTimeFormat,
        TextUtil.serializeList(props.destinationFields),
        props.sourceHeader,
        props.sourceDelimiter)

      cmd = SparkSubmitBuilder.build(submitMeta)
      val result = executeSparkSubmit(meta.search, cmd)

      if (result._1 == 0) {
        if (props.sourceRemove)
          hdfsIO ! RemoveDirectory(hdfsClient, new Path(props.sourcePath))
        context.parent ! TransactionSuccess(path)
      } else
        context.parent ! TransactionFailed(path, result._2.mkString)

      hivePool ! DisposeConnection(hiveConn)

    case Failure(e) => context.parent ! TransactionFailed(path, e.getMessage)
  }

  private def executeSparkSubmit(search: String, cmd: Seq[String]): (Int, StringBuilder) = {
    val builder = new StringBuilder()
    log.info(s"spark-submit: $cmd")
    val result = cmd ! ProcessLogger(log => {
      if(log.contains(search))
        builder.append(s"$log\n")
    })

    (result, builder)
  }
}
