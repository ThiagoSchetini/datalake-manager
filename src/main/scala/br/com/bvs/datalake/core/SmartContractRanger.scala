package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import scala.collection.mutable
import scala.concurrent.Await
import org.apache.hadoop.fs.{FileSystem, Path}

import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.core.HdfsPool.{Appendable, GetAppendable}
import br.com.bvs.datalake.core.SmartContractRanger.{TransactionFailed, TransactionSuccess}
import br.com.bvs.datalake.exception.SmartContractException
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO._
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.model.meta.CoreMetadata
import br.com.bvs.datalake.model.property.FileToHiveProps
import br.com.bvs.datalake.transaction.FileToHiveTransaction
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start
import br.com.bvs.datalake.builder.{PathBuilder, SmartContractBuilder}

object SmartContractRanger {
  def props(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef): Props =
    Props(new SmartContractRanger(hdfsClient, hdfsPool, hivePool, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
  case class TransactionSuccess(ongoingPath: Path)
  case class TransactionFailed(ongoingPath: Path, errorLog: String)
}

class SmartContractRanger(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef)
  extends Actor with ActorLogging {
  private var meta: CoreMetadata = _
  private var hdfsIO: ActorRef = _
  private var ongoingSmarts: mutable.HashMap[Path, (ActorRef, SmartContract)] = _
  private var appendables: mutable.HashMap[String, Appendable] = _

  override def preStart(): Unit = {
    meta = PropertiesHelper.getCoreMetadata
    implicit val clientTimeout: Timeout = meta.clientTimeout
    ongoingSmarts = new mutable.HashMap[Path, (ActorRef, SmartContract)]()
    appendables = new mutable.HashMap[String, Appendable]()
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")

    meta.smHdfsWatch.foreach(dir => {
      hdfsIO ! CheckOrCreateDir(hdfsClient, dir)
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.failDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.ongoingDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.doneDirName}")
      ernesto ! WatchSmartContractsOn(dir, meta.smWatchTick)

      /* only first runtime check on ongoing, in case of application break */
      hdfsIO ! ListFilesFrom(hdfsClient, s"$dir/${meta.ongoingDirName}")
    })

    // TODO appenders will be replaced by relational database
    val futureSmAppender = hdfsPool ? GetAppendable(meta.smHdfsDestiny)
    try {
      val smAppendable = Await.result(futureSmAppender, meta.clientTimeout.duration).asInstanceOf[Appendable]
      appendables += "sm" -> smAppendable
    } catch {
      case e: Exception =>
        context.parent ! Failure(new Exception(s"couldn't create HDFS appender: ${e.getMessage}"))
    }

    val futureFileToHiveAppender = hdfsPool ? GetAppendable(meta.fileToHiveHdfsDestiny)
    try {
      val fileToHiveAppendable = Await.result(futureFileToHiveAppender, meta.clientTimeout.duration).asInstanceOf[Appendable]
      appendables += "FileToHiveTransaction" -> fileToHiveAppendable
    } catch {
      case e: Exception =>
        context.parent ! Failure(new Exception(s"couldn't create HDFS appender: ${e.getMessage}"))
    }

  }

  override def receive: Receive = {
    case PathsList(paths) => onList(paths)

    case DataFromFile(smPath, smData) => onDataReceived(smPath, smData)

    case TransactionSuccess(path) => onTransactionSuccess(path)

    case TransactionFailed(path, errorLog) => onTransactionFail(path, errorLog)

    case Failure(e) => context.parent ! Failure(e)
  }

  private def onList(paths: List[Path]): Unit = {
    if (paths.nonEmpty) {
      paths
        .filter(_.getName.contains(meta.smSufix))
        .foreach(p => {
          log.info(s"reading ${p.getName}")
          hdfsIO ! ReadFile(hdfsClient, p)
        })
    }
  }

  private def onDataReceived(smPath: Path, smData: String): Unit = {

    /* ensures that it's using the ongoing path, never the original */
    val ongoingPath = PathBuilder.buildOngoingPath(smPath, meta.ongoingDirName)

    if (ongoingSmarts.contains(ongoingPath)) {
      log.warning(s"already on transaction $ongoingPath")
      hdfsIO ! RemoveFile(hdfsClient, smPath)

    } else {
      hdfsIO ! MoveTo(hdfsClient, smPath, ongoingPath)

      try {
        val sm = SmartContractBuilder.build(smData)

        val transactionName = sm.transactionProps.get.transactionName
        val transaction = createTransaction(transactionName, ongoingPath, sm)

        if (transaction == null) {
          onTransactionFail(ongoingPath, s"transaction $transactionName does not exist")

        } else {
          ongoingSmarts += ongoingPath -> (transaction, sm)
          transaction ! Start
        }

      } catch {
        /* catches exception of SmartContractBuilder.build(smData) */
        case e: SmartContractException => onTransactionFail(ongoingPath, e.getMessage)
      }
    }

  }

  private def onTransactionFail(ongoingPath: Path, cause: String): Unit = {
    val failPath = PathBuilder.buildFailPath(ongoingPath, meta.failDirName)
    val errorFilePath = PathBuilder.buildErrorFilePath(ongoingPath, meta.smSufix, meta.failDirName)

    hdfsIO ! MoveTo(hdfsClient, ongoingPath, failPath)
    hdfsIO ! OverwriteFileWithData(hdfsClient, errorFilePath, cause)

    if(ongoingSmarts.contains(ongoingPath)) {
      context.stop(ongoingSmarts(ongoingPath)._1)
      ongoingSmarts.remove(ongoingPath)
    }

    log.error(s"fail: $ongoingPath, $cause")
  }

  private def onTransactionSuccess(ongoingPath: Path): Unit = {
    val donePath = PathBuilder.buildDonePath(ongoingPath, meta.doneDirName)
    hdfsIO ! MoveTo(hdfsClient, ongoingPath, donePath)

    val ongoingSm = ongoingSmarts(ongoingPath)
    hdfsIO ! Append(appendables("sm").appender, ongoingSm._2.serializeToCSV)

    val transactionName = ongoingSm._2.transactionProps.get.transactionName
    hdfsIO ! Append(appendables(transactionName).appender, ongoingSm._2.transactionProps.get.autoSerializeCSV)

    context.stop(ongoingSm._1)
    ongoingSmarts.remove(ongoingPath)
    log.info(s"success: $ongoingPath")
  }

  private def createTransaction(transactionName: String, path: Path, sm: SmartContract): ActorRef = {

    transactionName match {

      case "FileToHiveTransaction" =>
        context.actorOf(FileToHiveTransaction.props(
          path,
          sm.transactionProps.get.asInstanceOf[FileToHiveProps],
          this.hdfsClient,
          this.hivePool,
          meta.clientTimeout),
          s"${sm.hash}")

      case _ => null
    }
  }

}
