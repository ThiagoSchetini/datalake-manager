package br.com.bvs.datalake.helper

import java.io.FileInputStream
import java.util.Properties
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.util.Timeout

import br.com.bvs.datalake.model.meta.{CoreMetadata, SparkMetadata}

object PropertiesHelper {
  private val datalakeManagerProps = sys.env.get("DATALAKE_MANAGER_PROPS").mkString
  private val datalakeSparkJars = sys.env.get("DATALAKE_SPARK_JARS").mkString

  private def readProperties(name: String) = {
    val props = new Properties()
    props.load(new FileInputStream(s"$datalakeManagerProps/$name.properties"))
    props
  }

  private def getCoreProps: Properties = {
    readProperties("core")
  }

  private def getSparkProps: Properties = {
    readProperties("spark")
  }

  def getKerberosProps: Properties = {
    readProperties("kerberos")
  }

  def getCoreMetadata: CoreMetadata = {
    val props = getCoreProps

    CoreMetadata(
      Timeout(props.getProperty("client.seconds.timeout").toInt seconds),
      props.getProperty("hadoop.conf.dir"),
      props.getProperty("hiveserver2.url"),
      props.getProperty("hiveserver2.pool.factor").toInt,
      props.getProperty("sm.hdfs.watch").split(",").toSet,
      Timeout(props.getProperty("sm.watch.seconds.tick").toInt seconds),
      props.getProperty("sm.sufix"),
      props.getProperty("shutdown.signal.dir"),
      props.getProperty("shutdown.signal.file"),
      Timeout(props.getProperty("shutdown.watch.seconds.tick").toInt seconds),
      props.getProperty("fail.dir.name"),
      props.getProperty("ongoing.dir.name"),
      props.getProperty("done.dir.name"),
      props.getProperty("sm.hdfs.destiny"),
      props.getProperty("filetohive.hdfs.destiny")
    )
  }

  def getSparkMetadata: SparkMetadata = {
    val props = getSparkProps

    SparkMetadata(
      props.getProperty("spark.submit.production").toBoolean,
      props.getProperty("spark.log.search"),
      props.getProperty("spark.submit.cmd"),
      props.getProperty("spark.deploy.mode"),
      s"$datalakeSparkJars/${props.getProperty("spark.jar")}",
      props.getProperty("yarn.queue")
    )
  }

}