package br.com.bvs.datalake.core

import java.io.ByteArrayInputStream
import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, Properties}
import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import br.com.bvs.datalake.core.SmartContractBuilder.{Build, Built}
import br.com.bvs.datalake.exception.SmartContractException
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.util.TextUtil
import org.apache.hadoop.fs.Path

object SmartContractBuilder {
  def props: Props = Props(new SmartContractBuilder)

  case class Build(smData: String, ongoingPath: Path)
  case class Built(sm: SmartContract, ongoingPath: Path)
}

class SmartContractBuilder extends Actor {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def receive: Receive = {

    case Build(smData, ongoingPath) =>
      try {
        val props = createProperties(smData)
        val sm = createSmartContract(props)
        val finalSM = addTransaction(sm, props)

        sender ! Built(finalSM, ongoingPath)

      } catch {
        case e: SmartContractException =>
          e.getMessage
          // TODO sender ! ERRO
      }
  }

  private def createProperties(smData: String): Properties = {
    val props = new Properties()
    props.load(new ByteArrayInputStream(smData.getBytes()))
    props
  }

  private def createSmartContract(props: Properties): SmartContract = {
    val smRequester = props.getProperty("sm.requester")
    val smAuthorizing = props.getProperty("sm.authorizing")

    if(TextUtil.isNotNullAndNotEmpty(smRequester) && TextUtil.isNotNullAndNotEmpty(smAuthorizing))
      SmartContract(hashIt(smRequester, smAuthorizing), new Timestamp(new Date().getTime), smRequester, smAuthorizing, None)
    else
      throw new SmartContractException("Missing information of sm requester/authorizing")
  }

  private def addTransaction(sm: SmartContract, props: Properties): SmartContract = {
    val transactionName = props.getProperty("transaction.name")

    if(TextUtil.isNotNullAndNotEmpty(transactionName)) {

      // TODO retornar um novo SM, agora com a transaction ... usar a propria case class




    } else {
      throw new SmartContractException("Missing transaction name")
    }
  }

  private def hashIt(str1: String, str2: String): String = {
    val bytesTime = Instant.now.toString.getBytes
    val bytes = bytesTime ++ str1.getBytes ++ str2.getBytes
    val digest = MessageDigest.getInstance("MD5").digest(bytes)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }

}
