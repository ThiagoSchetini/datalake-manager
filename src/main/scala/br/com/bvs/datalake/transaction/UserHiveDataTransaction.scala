package br.com.bvs.datalake.transaction

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import br.com.bvs.datalake.model.SmartContract
import br.com.bvs.datalake.transaction.UserHiveDataTransaction.Start
import org.apache.hadoop.fs.Path

object UserHiveDataTransaction {
  def props(path: Path, sm: SmartContract):Props = Props(new UserHiveDataTransaction(path, sm))

  case object Start
  case class HiveDataOk(path: Path)
  case class HiveDataFailed(path: Path)
}

class UserHiveDataTransaction(path: Path, sm: SmartContract) extends Actor with ActorLogging {

  override def receive: Receive = {
    case Start => {
      log.info(s"start: ${path.getName}")

    }

    case Failure(e) => {
      println(e.getMessage)
      log.info(s"Transaction failed for: ${path.getName}")
    }

  }
}
