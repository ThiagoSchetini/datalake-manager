package br.com.bvs.datalake.io

import java.sql.Connection
import akka.actor.{Actor, ActorLogging, Props, Status}
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseNotExist, DatabaseAndTableChecked}

object HiveIO {
  def props: Props = Props(new HiveIO)

  case class CheckTable(conn: Connection, database: String, table: String)
  case object DatabaseAndTableChecked
}


class HiveIO extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case CheckTable(conn, database, table) =>

      // TODO if table and database does not exists create it and respond:
      val stmt = conn.createStatement()

      val checkDatabase = s"create database if not exists $database;"
      val response = stmt.execute(checkDatabase)

      val checkTable = s""
      val tableName = s"$database.$table"

      sender ! DatabaseAndTableChecked

  }
}
