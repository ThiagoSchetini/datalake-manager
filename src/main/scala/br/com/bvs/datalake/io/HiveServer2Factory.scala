package br.com.bvs.datalake.io

import akka.actor.{Actor, ActorLogging, Props, Status}
import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import br.com.bvs.datalake.helper.CorePropertiesHelper
import br.com.bvs.datalake.io.HiveServer2Factory.GetHiveConnection
import br.com.bvs.datalake.model.CoreMetadata

object HiveServer2Factory {
  val props: Props = Props(new HiveServer2Factory)

  case object GetHiveConnection
}

class HiveServer2Factory extends Actor with ActorLogging{
  private var meta: CoreMetadata = _
  private var hiveConnection: Connection = _

  override def preStart(): Unit = {
    meta = CorePropertiesHelper.getCoreMetadata
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case GetHiveConnection =>
      if (hiveConnection == null) {
        Class.forName(meta.hiveDriverName)
        hiveConnection = DriverManager.getConnection(meta.hiveServer2URL)

        /* test the connection and throws Exception if don't connect */
        val stmt = hiveConnection.createStatement
        val tableName = "testdb.types"
        stmt.execute("drop table if exists " + tableName)
        stmt.execute("create table " + tableName + " (key int, value string)")
        val sql = "select * from "+ tableName
        println("Running: " + sql)
        val res = stmt.executeQuery(sql)
        println("query OK!")
        println(res)

      } else {
        /* test the connection and throws Exception if don't connect */
        val stmt = hiveConnection.createStatement
        val tableName = "testdb.types"
        stmt.execute("drop table if exists " + tableName)
        stmt.execute("create table " + tableName + " (key int, value string)")
        val sql = "select * from "+ tableName
        println("Running: " + sql)
        val res = stmt.executeQuery(sql)
        println("query OK!")
        println(res)

        // https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC
      }
      sender ! hiveConnection
  }

}
