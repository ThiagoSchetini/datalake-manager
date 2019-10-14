package br.com.bvs.datalake.contract

import java.io.FileInputStream
import java.util.Properties
import akka.actor.{Actor, ActorLogging, Props}
import br.com.bvs.datalake.contract.SmartContractRanger.ReadSmartContract

object SmartContractRanger {
  def props: Props = Props(new SmartContractRanger)

  case class ReadSmartContract(directory: String, filename: String)
}

class SmartContractRanger extends Actor with ActorLogging {

  override def receive: Receive = {
    case ReadSmartContract(directory, filename) =>
      val props = new Properties()
      props.load(new FileInputStream(s"$directory/$filename"))
      val builder = new SmartContractBuilder(props)
      val smartContract = builder.build
      log.info(s"smart created: $smartContract")
  }
}
