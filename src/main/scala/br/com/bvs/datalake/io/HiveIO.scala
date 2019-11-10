package br.com.bvs.datalake.io

import java.sql.Connection
import akka.actor.{Actor, ActorLogging, Props, Status}
import br.com.bvs.datalake.io.HiveIO.{CheckTable, DatabaseAndTableChecked}

object HiveIO {
  def props: Props = Props(new HiveIO)

  case class CheckTable(conn: Connection, database: String, table: String, location: String, fields: List[(String, String)])
  case object DatabaseAndTableChecked
}

class HiveIO extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case CheckTable(conn, database, table, location, fields) =>
      val stmt = conn.createStatement()

      val checkDatabase = s"create database if not exists $database"
      val databaseResponse = stmt.execute(checkDatabase)
      if(databaseResponse)
        log.warning(s"database created: $database")

      val createTable = createTableQuery(database, table, location, fields)
      val tableResponse = stmt.execute(createTable)
      if(tableResponse)
        log.warning(s"table created: $database.$table:")
        log.warning(createTable)

      sender ! DatabaseAndTableChecked
  }

  def createTableQuery(database: String, table: String, location: String, fields: List[(String, String)]): String = {
    val builder = new StringBuilder
    builder
      .append(s"create external table if not exists $database.$table (")

    /* fields part */
    val fieldz = fields.reverse.tail.reverse
    val last = fields.last
    fieldz.foreach(tuple => builder.append(s"${tuple._1} ${tuple._2},"))
    builder.append(s"${last._1} ${last._2}")

    /* config part */
    builder.append(") ")
      .append("stored as parquet ")
      .append(s"location '$location' ")
      .append("tblproperties ('parquet.block.size'='134217728')")

    builder.mkString
  }
}

