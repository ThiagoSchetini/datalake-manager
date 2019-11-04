package br.com.bvs.datalake.helper

import java.io.FileInputStream
import java.util.Properties

import scala.concurrent.duration._
import akka.util.Timeout
import br.com.bvs.datalake.model.CoreMetadata

import scala.language.postfixOps

object AppPropertiesHelper {
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
      props.getProperty("hadoop.conf.dir"),
      Timeout(props.getProperty("hdfs.client.seconds.timeout").toInt seconds),
      props.getProperty("fail.dir.name"),
      props.getProperty("ongoing.dir.name"),
      props.getProperty("sm.watch.dirs").split(",").toSet,
      Timeout(props.getProperty("sm.watch.seconds.tick").toInt seconds),
      props.getProperty("sm.sufix")
    )
  }
}