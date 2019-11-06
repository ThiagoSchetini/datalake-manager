package br.com.bvs.datalake.io

import java.sql.Connection
import akka.actor.{Actor, ActorLogging, Props, Status}
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseNotExist, TableChecked}

object HiveIO {
  def props: Props = Props(new HiveIO)

  case class CheckTable(conn: Connection, database: String, table: String)
  case object DatabaseNotExist
  case object TableChecked
}


class HiveIO extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case CheckTable(conn, database, table) =>

      // TODO check hive database.
      sender ! DatabaseNotExist

      // TODO if table does not exists create it and responsd:
      sender ! TableChecked

  }
}
