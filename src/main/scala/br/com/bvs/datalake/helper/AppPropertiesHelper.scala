package br.com.bvs.datalake.helper

import java.io.FileInputStream
import java.util.Properties

import scala.concurrent.duration._
import akka.util.Timeout
import br.com.bvs.datalake.model.SupervisorMetadata

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

  def getAppProps: Properties = {
    readProperties("app")
  }

  def getSupervisorMetadata: SupervisorMetadata = {
    val props = readProperties("supervisor")

    SupervisorMetadata(
      props.getProperty("ongoingDirectory"),
      props.getProperty("failDirectory"),
      Timeout(props.getProperty("hdfsClientTimeout").toInt seconds))
  }

}
