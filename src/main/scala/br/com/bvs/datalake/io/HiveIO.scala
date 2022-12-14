package br.com.bvs.datalake.io

import java.sql.Connection
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import br.com.bvs.datalake.io.HiveIO.{CheckTable, TableChecked}

object HiveIO {
  def props: Props = Props(new HiveIO)

  case class CheckTable(conn: Connection, database: String, table: String, location: String, fields: List[(String, String)])
  case object TableChecked
}

class HiveIO extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Failure(reason)
  }

  override def receive: Receive = {
    case CheckTable(conn, database, table, location, fields) =>
      val databaseQuery = s"create database if not exists $database"
      val tableQuery = buildQueryCreateTable(database, table, location, fields)

      val stmt = conn.createStatement()
      stmt.execute(databaseQuery)
      stmt.execute(tableQuery)
      sender ! TableChecked
  }

  private def buildQueryCreateTable(database: String, table: String, location: String, fields: List[(String, String)]): String = {
    val builder = new StringBuilder

    /* create part */
    builder.append(s"create external table if not exists $database.$table (")

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

