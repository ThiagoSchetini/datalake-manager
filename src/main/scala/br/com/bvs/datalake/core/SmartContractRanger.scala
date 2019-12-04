package br.com.bvs.datalake.core

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask

import scala.collection.mutable
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.ByteArrayInputStream
import java.util.{Date, Properties}
import java.security.MessageDigest
import java.math.BigInteger
import java.sql.Timestamp
import java.time.Instant

import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.core.HdfsPool.{Appendable, GetAppendable}
import br.com.bvs.datalake.core.SmartContractRanger.{TransactionFailed, TransactionSuccess}
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO._
import br.com.bvs.datalake.model.{CoreMetadata, SmartContract}
import br.com.bvs.datalake.transaction.FileToHiveTransaction
import br.com.bvs.datalake.transaction.FileToHiveTransaction.Start
import br.com.bvs.datalake.util.TextUtil

import scala.concurrent.Await

object SmartContractRanger {
  def props(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef): Props =
    Props(new SmartContractRanger(hdfsClient, hdfsPool, hivePool, ernesto))

  case class ReadSmartContract(directory: String, filename: String)
  case class TransactionSuccess(ongoingPath: Path)
  case class TransactionFailed(ongoingPath: Path, errorLog: String)
}

class SmartContractRanger(hdfsClient: FileSystem, hdfsPool: ActorRef, hivePool: ActorRef, ernesto: ActorRef)
  extends Actor with ActorLogging {
  private var ongoingSm: mutable.HashMap[Path, (ActorRef, StringBuilder)] = _
  private var meta: CoreMetadata = _
  private var hdfsIO: ActorRef = _
  private var smAppendable: Appendable = _

  override def preStart(): Unit = {
    meta = PropertiesHelper.getCoreMetadata
    implicit val clientTimeout: Timeout = meta.clientTimeout
    ongoingSm = new mutable.HashMap[Path, (ActorRef, StringBuilder)]()

    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")

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

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    // TODO remove this after the sm validation is complete: this actor can't broke!
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {
    case PathsList(paths) => onSmartContractsList(paths)

    case DataFromFile(smPath, smData) => onSmartContractDataReceived(smPath, smData)

    case TransactionSuccess(path) => onTransactionSuccess(path)

    case TransactionFailed(path, errorLog) => onTransactionFail(path, errorLog)

    case Failure(e) => context.parent ! Failure(e)
  }

  private def onSmartContractsList(paths: List[Path]): Unit = {
    if (paths.nonEmpty) {
      paths
        .filter(_.getName.contains(meta.smSufix))
        .foreach(p => {
          log.info(s"reading smart contract ${p.getName}")
          hdfsIO ! ReadFile(hdfsClient, p)
        })
    }
  }

  private def onSmartContractDataReceived(smPath: Path, smData: String): Unit = {
    val props = new Properties()
    props.load(new ByteArrayInputStream(smData.getBytes()))
    val sm = buildSmartContract(props)

    val check = validateSmartContract(sm)
    if (!check) {
      onTransactionFail(smPath, "sm not valid")

    } else {
      val ongoingSmPath = isOngoingPath(smPath) match {
        case true => smPath
        case false => buildOngoingPath(smPath)
      }

      hdfsIO ! MoveTo(hdfsClient, smPath, ongoingSmPath)

      val hash = buildHash(smData.getBytes())
      val transaction = createTransaction(sm.transaction, ongoingSmPath, sm, hash)

      if (transaction == null) {
        onTransactionFail(ongoingSmPath, s"transaction ${sm.transaction} is not valid")
      } else {
        ongoingSm += ongoingSmPath -> (transaction, serializeSmartContract(ongoingSmPath.getName, sm, hash))
        transaction ! Start
      }
    }
  }

  private def onTransactionFail(ongoingPath: Path, cause: String): Unit = {
    val errorPath = buildTargetPath(ongoingPath, meta.failDirName)
    val errorFilePath = buildErrorFilePath(ongoingPath, errorPath)

    hdfsIO ! MoveTo(hdfsClient, ongoingPath, errorPath)
    hdfsIO ! OverwriteFileWithData(hdfsClient, errorFilePath, cause)

    if(ongoingSm.contains(ongoingPath)) {
      context.stop(ongoingSm(ongoingPath)._1)
      ongoingSm.remove(ongoingPath)
    }

    log.error(s"failed: $ongoingPath, $cause")
  }

  private def onTransactionSuccess(ongoingPath: Path): Unit = {
    val donePath = buildTargetPath(ongoingPath, meta.doneDirName)
    hdfsIO ! MoveTo(hdfsClient, ongoingPath, donePath)
    hdfsIO ! Append("sm", smAppendable.appender, ongoingSm(ongoingPath)._2)
    context.stop(ongoingSm(ongoingPath)._1)
    ongoingSm.remove(ongoingPath)
    log.info(s"success: $ongoingPath")
  }

  private def createTransaction(transaction: String, path: Path, sm: SmartContract, hash: String): ActorRef = {
    transaction match {
      case "FileToHiveTransaction" =>
        context.actorOf(FileToHiveTransaction.props(path, sm, this.hdfsClient, this.hivePool, meta.clientTimeout), s"transaction-$hash")

      case _ => null
    }
  }

  private def buildSmartContract(props: Properties): SmartContract = {
    SmartContract(
      props.getProperty("source.server"),
      props.getProperty("source.path"),
      props.getProperty("source.header").toBoolean,
      props.getProperty("source.delimiter"),
      props.getProperty("source.remove").toBoolean,
      props.getProperty("source.time.format"),
      props.getProperty("destination.fields").split(",").toList,
      props.getProperty("destination.types").split(",").toList,
      props.getProperty("destination.path"),
      props.getProperty("destination.database"),
      props.getProperty("destination.table"),
      props.getProperty("destination.overwrite").toBoolean,
      props.getProperty("transaction")
    )
  }

  private def validateSmartContract(sm: SmartContract): Boolean = {
    // TODO create validations
    true
  }

  private def serializeSmartContract(smFileName: String, sm: SmartContract, hash: String): StringBuilder = {
    val newline = "\n"
    val smBuilder = new StringBuilder()

    smBuilder.append(
      s"""$hash
         |${new Timestamp(new Date().getTime)}
         |$smFileName
         |${sm.transaction}
         |${sm.sourceServer}
         |${sm.sourcePath}
         |${sm.destinationPath}
         |${sm.destinationDatabase}
         |${sm.destinationTable}
         |${TextUtil.serializeList(sm.destinationFields)}
         |${TextUtil.serializeList(sm.destinationTypes)}
         |${sm.destinationOverwrite}"""
        .stripMargin.replaceAll(newline, "|")).append(newline)

    smBuilder
  }

  private def buildHash(array: Array[Byte]): String = {
    val bytesTime = Instant.now.toString.getBytes
    val sum = array ++ bytesTime
    val digest = MessageDigest.getInstance("MD5").digest(sum)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }

  private def isOngoingPath(smPath: Path): Boolean = {
    smPath.getParent.getName == meta.ongoingDirName
  }

  private def buildOngoingPath(smPath: Path): Path = {
    new Path(s"${smPath.getParent}/${meta.ongoingDirName}/${smPath.getName}")
  }

  private def buildTargetPath(ongoingPath: Path, target: String): Path = {
    new Path(s"${ongoingPath.getParent.getParent}/$target")
  }

  private def buildErrorFilePath(ongoingPath: Path, errorPath: Path): Path = {
    val name = ongoingPath.getName.replace(s".${meta.smSufix}", "")
    val errorName = s"$name.error"
    new Path(s"$errorPath/$errorName")
  }
}
