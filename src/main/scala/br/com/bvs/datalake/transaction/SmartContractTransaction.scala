package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import br.com.bvs.datalake.io.HdfsIO
import br.com.bvs.datalake.transaction.SmartContractTransaction.Start

object SmartContractTransaction {
  def props(sm: String):Props = Props(new SmartContractTransaction(sm))

  case object Start
}

class SmartContractTransaction(sm: String) extends Actor with ActorLogging {
  var hdfsIO: ActorRef = _

  override def preStart(): Unit = {
    hdfsIO = context.actorOf(HdfsIO.props, "hdfs-io")
  }

  override def receive: Receive = {
    case Start => {

    }

    case Failure(e) => {
      println(e.getMessage)
      // TODO move sm to fail and create a file with the exception
    }

  }
}
