package br.com.bvs.datalake.builder

import java.io.ByteArrayInputStream
import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, Properties}

import br.com.bvs.datalake.exception.SmartContractException
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.model.property.{FileToHiveProps, TransactionProps}
import br.com.bvs.datalake.util.TextUtil

object SmartContractBuilder {

  def build(smData: String): SmartContract = {
    try {
      val props = createProperties(smData)
      val sm = createSmartContract(props)
      createTransaction(sm, props)

    } catch {
      case e: Exception => throw new SmartContractException(s"sm malformed data: ${e.getMessage}")
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
      SmartContract(
        hashIt(smRequester, smAuthorizing),
        new Timestamp(new Date().getTime),
        smRequester,
        smAuthorizing,
        None)
    else
      throw new Exception("requester/authorizing")
  }

  private def createTransaction(sm: SmartContract, props: Properties): SmartContract = {
    val transactionName = props.getProperty("transaction.name")

    if(!validateTransactionName(transactionName))
      throw new Exception("transaction name")

      /* there is a Number or Data Exception risk at creation of any TransactionProps object */
      val transactionProps = createTransactionProps(transactionName, props)

      if(!validateTransaction(transactionProps))
        throw new Exception("transaction data")

      SmartContract(
        sm.hash,
        sm.creationTime,
        sm.smRequester,
        sm.smAuthorizing,
        Some(transactionProps))
  }

  private def hashIt(str1: String, str2: String): String = {
    val bytesTime = Instant.now.toString.getBytes
    val bytes = bytesTime ++ str1.getBytes ++ str2.getBytes
    val digest = MessageDigest.getInstance("MD5").digest(bytes)
    val bigInteger = new BigInteger(1, digest)
    bigInteger.toString(16)
  }

  private def validateTransactionName(transactionName: String): Boolean = {
    TextUtil.isNotNullAndNotEmpty(transactionName)
  }

  private def createTransactionProps(transactionName: String, props: Properties): TransactionProps = {
    transactionName match {
      case "FileToHiveTransaction" => FileToHiveProps(transactionName, props)
      case _ => null
    }
  }

  private def validateTransaction(transactionProps: TransactionProps): Boolean = {
    if (transactionProps == null || !transactionProps.autoValidateNullOrEmpty)
      false
    else
      true
  }

}
