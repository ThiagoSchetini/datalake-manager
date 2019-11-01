package br.com.bvs.datalake.core

import java.io.FileInputStream
import java.util.Properties
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import br.com.bvs.datalake.core.Ernesto.WatchSmartContractsOn
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.model.SmartContract
import org.apache.hadoop.fs.FileSystem

object SmartContractRanger {
  def props(hdfsClient: FileSystem, ernesto: ActorRef): Props = Props(new SmartContractRanger(hdfsClient, ernesto))

  case class ReadSmartContract(directory: String, filename: String)

}

class SmartContractRanger(hdfsClient: FileSystem, ernesto: ActorRef) extends Actor with ActorLogging {
  var hdfsIO: ActorRef = _

  override def preStart(): Unit = {
    hdfsIO = context.actorOf(HdfsIO.props)

    // TODO como o ranger vai atras dos arquivos ? properties... fixo...
    ernesto ! WatchSmartContractsOn("src/test/mocks")

  }

  override def receive: Receive = {
    case ReadSmartContract(directory, filename) =>
      val props = new Properties()
      props.load(new FileInputStream(s"$directory/$filename"))
      val smartContract = new SmartContractBuilder(props).build
      log.info(s"smart read: $smartContract")
      self ! ValidadeSmartContract(smartContract)


    /*

    if(smarts.isEmpty) {
      // TODO only for the first version v0.1 !
      log.info("There is no smart contracts anymore to read")
      reaper ! Reap
    } else {
      smarts.foreach(f => {
        // hdfsio move
        //fileSecureMove(s"$directory/$f", s"$directory/$ongoingSubName/$f")
        smartContractRanger ! ReadSmartContract(s"$directory/$ongoingSubName", f)
      })
    }

    val smarts = new File(directory)
      .filter(_.getName.endsWith(smartContractSufix))
      .map(_.getName)
      .toSet
*/

      // validadeSmartContract()
      // serializeSmartContract()
      // hdfsIO ! Upload()




  }

  private def buildSmartContract(props: Properties): SmartContract = {
    SmartContract(props.getProperty("sourceName"),
      props.getProperty("sourceServer"),
      props.getProperty("sourcePath"),
      props.getProperty("sourceFields").split(",").toSet,
      props.getProperty("destinationFields").split(",").toSet,
      props.getProperty("destinationTypes").split(",").toSet,
      props.getProperty("fileReleasePath"),
      props.getProperty("smartReleasePath"),
      props.getProperty("distributionPaths").split(",").toSet,
      props.getProperty("versionPattern"),
      props.getProperty("delimiter"),
      props.getProperty("header")
    )
  }

  private def validadeSmartContract(sm: SmartContract): SmartContract = {
    // TODO
    // val destiny = smartContract.smartReleasePath
    ???
  }

  private def serializeSmartContract(sm: SmartContract): String = {
    // TODO
    // serializar para CSV
    ???
  }


}
