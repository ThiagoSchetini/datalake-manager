package br.com.bvs.datalake.helper

import java.io.FileInputStream
import java.util.Properties

import scala.concurrent.duration._
import akka.util.Timeout
import br.com.bvs.datalake.model.CoreMetadata

import scala.language.postfixOps

object CorePropertiesHelper {
  private val appPropertiesPath = sys.env.get("DATALAKE_MANAGER_PROPS").mkString

  private def readProperties(name: String) = {
    val props = new Properties()
    props.load(new FileInputStream(s"$appPropertiesPath/$name.properties"))
    props
  }

  def getKerberosProps: Properties = {
    readProperties("kerberos")
  }

  private def getCoreProps: Properties = {
    readProperties("core")
  }

  def getCoreMetadata: CoreMetadata = {
    val props = getCoreProps

    CoreMetadata(
      Timeout(props.getProperty("client.seconds.timeout").toInt seconds),
      props.getProperty("hadoop.conf.dir"),
      props.getProperty("hive.driver.name"),
      props.getProperty("hiveserver2.url"),
      props.getProperty("fail.dir.name"),
      props.getProperty("ongoing.dir.name"),
      props.getProperty("done.dir.name"),
      props.getProperty("sm.watch.dirs").split(",").toSet,
      Timeout(props.getProperty("sm.watch.seconds.tick").toInt seconds),
      props.getProperty("sm.sufix"),
      props.getProperty("sm.delimiter")
    )
  }
}