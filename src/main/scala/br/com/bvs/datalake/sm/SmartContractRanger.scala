package br.com.bvs.datalake.sm

import java.io.FileInputStream
import java.util.Properties
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import br.com.bvs.datalake.sm.SmartContractRanger.{ReadSmartContract, ValidadeSmartContract}
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.io.HdfsIO.Upload

object SmartContractRanger {
  def props: Props = Props(new SmartContractRanger)

  case class ReadSmartContract(directory: String, filename: String)
  case class ValidadeSmartContract(sm: SmartContract)
}

class SmartContractRanger(hdfsFactory: ActorRef) extends Actor with ActorLogging {
  var hdfsIO: ActorRef = _

  override def preStart(): Unit = {
    hdfsIO = context.actorOf(HdfsIO.props)
  }

  override def receive: Receive = {
    case ReadSmartContract(directory, filename) =>
      val props = new Properties()
      props.load(new FileInputStream(s"$directory/$filename"))
      val smartContract = new SmartContractBuilder(props).build
      log.info(s"smart read: $smartContract")
      self ! ValidadeSmartContract(smartContract)




    case ValidadeSmartContract(sm) =>
      sm.
      // TODO de alguma forma enviar para o hadoop... pensando em criar um Serializer
      // val destiny = smartContract.smartReleasePath
      // hdfsIO ! Upload() // Upload(hdfs: FileSystem, source: String, target: String)
  }


}
