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
import br.com.bvs.datalake.core.SmartContractBuilder.{Build, Built}
import br.com.bvs.datalake.core.SmartContractRanger.{TransactionFailed, TransactionSuccess}
import br.com.bvs.datalake.helper.{PathHelper, PropertiesHelper}
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO._
import br.com.bvs.datalake.model.{CoreMetadata, SmartContract}
import br.com.bvs.datalake.transaction.FileToHiveTransaction
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start

object SmartContractRanger {
  def props(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef): Props =
    Props(new SmartContractRanger(hdfsClient, hdfsPool, hivePool, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
  case class TransactionSuccess(ongoingPath: Path)
  case class TransactionFailed(ongoingPath: Path, errorLog: String)
}

class SmartContractRanger(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef)
  extends Actor with ActorLogging {
  private var ongoingSmarts: mutable.HashMap[Path, (ActorRef, SmartContract)] = _
  private var meta: CoreMetadata = _
  private var hdfsIO: ActorRef = _
  private var smBuilder: ActorRef = _
  private var smAppendable: Appendable = _

  override def preStart(): Unit = {
    meta = PropertiesHelper.getCoreMetadata
    implicit val clientTimeout: Timeout = meta.clientTimeout
    ongoingSmarts = new mutable.HashMap[Path, (ActorRef, SmartContract)]()

    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")
    smBuilder = context.actorOf(SmartContractBuilder.props)

    meta.smWatchHdfsDirs.foreach(dir => {
      hdfsIO ! CheckOrCreateDir(hdfsClient, dir)
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.failDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.ongoingDirName}")
      hdfsIO ! CheckOrCreateDir(hdfsClient, s"$dir/${meta.doneDirName}")
      ernesto ! WatchSmartContractsOn(dir, meta.smWatchTick)

      /* only first runtime check on ongoing, in case of application break */
      hdfsIO ! ListFilesFrom(hdfsClient, s"$dir/${meta.ongoingDirName}")
    })

    val futureAppender = hdfsPool ? GetAppendable(meta.smDestinyHdfsDir)
    try {
      smAppendable = Await.result(futureAppender, meta.clientTimeout.duration).asInstanceOf[Appendable]
    } catch {
      case e: Exception =>
        context.parent ! Failure(new Exception(s"couldn't create HDFS appender to smart contracts: ${e.getMessage}"))
    }
  }

  override def receive: Receive = {
    case PathsList(paths) => onList(paths)

    case DataFromFile(smPath, smData) => onDataReceived(smPath, smData)

    case Built(sm, ongoingPath) => onBuilt(sm, ongoingPath)

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
    val ongoingPath = PathHelper.buildOngoingPath(smPath, meta.ongoingDirName)

    if (ongoingSmarts.contains(ongoingPath)) {
      log.warning(s"already on transaction $ongoingPath")
      hdfsIO ! RemoveFile(hdfsClient, smPath)

    } else {
      hdfsIO ! MoveTo(hdfsClient, smPath, ongoingPath)
      smBuilder ! Build(smData, ongoingPath)
    }
  }

  private def onBuilt(sm: SmartContract, ongoingPath: Path): Unit = {
    val transaction = createTransaction(sm.transaction, ongoingPath, sm)

    if (transaction == null) {
      onTransactionFail(ongoingPath, s"transaction ${sm.transaction} is not valid")
    } else {
      ongoingSmarts += ongoingPath -> (transaction, sm)
      transaction ! Start
    }
  }

  private def onTransactionFail(ongoingPath: Path, cause: String): Unit = {
    val failPath = PathHelper.buildFailPath(ongoingPath, meta.failDirName)
    val errorFilePath = PathHelper.buildErrorFilePath(ongoingPath, meta.smSufix, meta.failDirName)

    hdfsIO ! MoveTo(hdfsClient, ongoingPath, failPath)
    hdfsIO ! OverwriteFileWithData(hdfsClient, errorFilePath, cause)

    if(ongoingSmarts.contains(ongoingPath)) {
      context.stop(ongoingSmarts(ongoingPath)._1)
      ongoingSmarts.remove(ongoingPath)
    }

    log.error(s"fail: $ongoingPath, $cause")
  }

  private def onTransactionSuccess(ongoingPath: Path): Unit = {
    val donePath = PathHelper.buildDonePath(ongoingPath, meta.doneDirName)
    hdfsIO ! MoveTo(hdfsClient, ongoingPath, donePath)

    val ongoingSm = ongoingSmarts(ongoingPath)
    hdfsIO ! Append("sm", smAppendable.appender, ongoingSm._2.serializeCSV)
    context.stop(ongoingSm._1)
    ongoingSmarts.remove(ongoingPath)
    log.info(s"success: $ongoingPath")
  }

  private def createTransaction(transaction: String, path: Path, sm: SmartContract): ActorRef = {
    transaction match {
      case "FileToHiveTransaction" =>
        context.actorOf(FileToHiveTransaction.props(path, sm, this.hdfsClient, this.hivePool, meta.clientTimeout), s"${sm.hash}")

      case _ => null
    }
  }

}
