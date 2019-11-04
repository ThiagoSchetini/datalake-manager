package br.com.bvs.datalake.io

import akka.actor.{Actor, ActorLogging, Props, Status}
import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import br.com.bvs.datalake.io.HiveConnectionFactory.GetHiveConnection

object HiveConnectionFactory {
  val props: Props = Props(new HiveConnectionFactory)

  case object GetHiveConnection
}

class HiveConnectionFactory extends Actor with ActorLogging{
  private val driverName = "org.apache.hive.jdbc.HiveDriver"
  private var hiveConnection: Connection = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case GetHiveConnection =>
      if (hiveConnection == null) {
        Class.forName(driverName)
        hiveConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "")

      } else {
        /* test the connection and throws Exception if don't connect */

      }
      sender ! hiveConnection
  }

}
