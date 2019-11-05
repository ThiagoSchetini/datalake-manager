package br.com.bvs.datalake.io

import akka.actor.{Actor, ActorLogging, Props, Status}
import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import br.com.bvs.datalake.helper.CorePropertiesHelper
import br.com.bvs.datalake.io.HivePool.{GetHiveConnection, DisposeConnection}
import br.com.bvs.datalake.model.CoreMetadata

import scala.collection.mutable

object HivePool {
  val props: Props = Props(new HivePool)

  case object GetHiveConnection
  case class DisposeConnection(conn: Connection)
}

class HivePool extends Actor with ActorLogging{
  private var meta: CoreMetadata = _
  private var connPool: mutable.Set[Connection] = _
  private var connBusy: mutable.Set[Connection] = _

  override def preStart(): Unit = {
    meta = CorePropertiesHelper.getCoreMetadata
    connPool = new mutable.HashSet[Connection]()
    connBusy = new mutable.HashSet[Connection]()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sender ! Status.Failure(reason)
  }

  override def receive: Receive = {
    case GetHiveConnection =>
      if (connPool.isEmpty)
        for (_ <- 1 to meta.hiveServer2PoolFactor) {
          val conn = DriverManager.getConnection(meta.hiveServer2URL)
          connPool += conn
          log.info(s"$conn")
        }

      val conn = connPool.head
      connPool -= conn
      connBusy += conn
      sender ! conn

      log.info(s"total pool available connections: ${connPool.size}")
      log.info(s"total pool using connections: ${connBusy.size}")

      /*
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
      */


    case DisposeConnection(conn: Connection) =>
      connBusy -= conn
      connPool += conn
      log.info(s"hive connection $conn returned to pool")

  }

  override def postStop(): Unit = {
    log.info("closing open connections")
    connPool.foreach(_.close)
    connBusy.foreach(_.close)
  }
}
