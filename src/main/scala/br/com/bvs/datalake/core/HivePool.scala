package br.com.bvs.datalake.core

import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Status.Failure
import scala.collection.mutable
import java.sql.{Connection, DriverManager}
import br.com.bvs.datalake.core.HivePool.{DisposeConnection, GetHiveConnection}
import br.com.bvs.datalake.helper.PropertiesHelper
import br.com.bvs.datalake.model.CoreMetadata

object HivePool {
  val props: Props = Props(new HivePool)

  case object GetHiveConnection
  case class DisposeConnection(conn: Connection)
}

class HivePool extends Actor with ActorLogging{
  private var meta: CoreMetadata = _
  private var connPool: mutable.Set[Connection] = _
  private var connBusy: mutable.Set[Connection] = _

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! Failure(reason)
  }

  override def preStart(): Unit = {
    meta = PropertiesHelper.getCoreMetadata
    connPool = new mutable.HashSet[Connection]()
    connBusy = new mutable.HashSet[Connection]()
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

      log.info(s"available connections: ${connPool.size}")
      log.info(s"using connections: ${connBusy.size}")

    case DisposeConnection(conn: Connection) =>
      connBusy -= conn
      connPool += conn
      log.info(s"hive connection $conn returned to pool")
  }

  override def postStop(): Unit = {
    log.info("closing opened connections")
    connPool.foreach(_.close)
    connBusy.foreach(_.close)
  }
}
