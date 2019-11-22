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
  case class TransactionSuccess(smPath: Path)
  case class TransactionFailed(smPath: Path, errorLog: String)
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
    // TODO remove this after the sm validation is complete
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {
    case PathsList(paths) => onSmartContractsList(paths)

    case DataFromFile(smPath, smData) => onSmartContractDataReceived(smPath, smData)

    case TransactionSuccess(path) => onTransactionSuccess(path)

    case TransactionFailed(path, errorLog) => onTransactionFail(path, errorLog)

    case Failure(e) => context.parent ! Failure(e)
  }

  private def createTransaction(transaction: String, path: Path, sm: SmartContract, hash: String): ActorRef = {
    transaction match {
      case "FileToHiveTransaction" =>
        context.actorOf(FileToHiveTransaction.props(path, sm, this.hdfsClient, this.hivePool, meta.clientTimeout), s"transaction-$hash")

      case _ => null
    }
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
      moveSmartContractToOngoing(smPath)
      val hash = hashSmartContract(smData.getBytes())
      val transaction = createTransaction(sm.transaction, smPath, sm, hash)

      if (transaction == null) {
        onTransactionFail(smPath, s"transaction ${sm.transaction} is not valid")
      } else {
        ongoingSm += smPath -> (transaction, serializeSmartContract(smPath.getName, sm, hash))
        transaction ! Start
      }
    }
  }

  private def onTransactionFail(smPath: Path, cause: String): Unit = {
    val tuple = buildOngoingAndTargetPaths(smPath, meta.failDirName)
    val errorPath = buildErrorFilePath(smPath, meta.failDirName)

    hdfsIO ! MoveTo(hdfsClient, tuple._1, tuple._2)
    hdfsIO ! OverwriteFileWithData(hdfsClient, errorPath, cause)

    if(ongoingSm.contains(smPath)) {
      context.stop(ongoingSm(smPath)._1)
      ongoingSm.remove(smPath)
    }

    log.error(s"failed: $smPath, $cause")
  }

  private def onTransactionSuccess(smPath: Path): Unit = {
    val tuple = buildOngoingAndTargetPaths(smPath, meta.doneDirName)
    hdfsIO ! MoveTo(hdfsClient, tuple._1, tuple._2)
    hdfsIO ! Append("sm", smAppendable.appender, ongoingSm(smPath)._2)
    context.stop(ongoingSm(smPath)._1)
    ongoingSm.remove(smPath)
    log.info(s"success: $smPath")
  }

  private def buildSmartContract(props: Properties): SmartContract = {
    SmartContract(
      props.getProperty("source.server"),
      props.getProperty("source.path"),
      props.getProperty("source.header").toBoolean,
      props.getProperty("source.delimiter"),
      props.getProperty("source.remove").toBoolean,
      props.getProperty("source.true.format"),
      props.getProperty("source.false.format"),
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

  private def isOngoing(smPath: Path): Boolean = {
    smPath.getParent.getName == meta.ongoingDirName
  }

  private def moveSmartContractToOngoing(smPath: Path): Unit = {
    if(!isOngoing(smPath))
      hdfsIO ! MoveToSubDir(hdfsClient, smPath, meta.ongoingDirName)
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
        .stripMargin.replaceAll(newline, meta.smDelimiter.toString)).append(newline)

    smBuilder
  }

  private def hashSmartContract(array: Array[Byte]): String = {
    val bytesTime = Instant.now.toString.getBytes
    val sum = array ++ bytesTime
    val digest = MessageDigest.getInstance("MD5").digest(sum)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }

  private def buildOngoingAndTargetPaths(smPath: Path, target: String): (Path, Path) = {
    /* we have to consider if the sm already comes from ongoing */
    var ongoingPath = smPath
    if (!isOngoing(smPath))
      ongoingPath = new Path(s"${smPath.getParent}/${meta.ongoingDirName}/${smPath.getName}")

    val targetPath = new Path(s"${ongoingPath.getParent.getParent}/$target/${ongoingPath.getName}")
    (ongoingPath, targetPath)
  }

  private def buildErrorFilePath(smPath: Path, errorDir: String): Path = {
    val name = smPath.getName.replace(s".${meta.smSufix}", "")
    val errorName = s"$name.error"
    new Path(s"${smPath.getParent}/$errorDir/$errorName")
  }
}
