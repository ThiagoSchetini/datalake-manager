package br.com.bvs.datalake.helper

import java.io.{File, FileInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

object HadoopConfHelper {

  @volatile private var lastConfiguration: Configuration = _

  def getConfiguration: Configuration = {
    if(lastConfiguration == null) {
      val environment = PropertiesHelper.getCoreMetadata.hadoopConfDir
      lastConfiguration = buildConfiguration(environment)
    }
    lastConfiguration
  }

  def getIOFileBufferSize: Int = {
    if (lastConfiguration != null && lastConfiguration.get("io.file.buffer.size") != null) {
      lastConfiguration.get("io.file.buffer.size").toInt
    } else {
      /* default is hadoop recommended 128 Kibibytes */
      131072
    }
  }

  private def buildConfiguration(environment: String): Configuration = {
    val configuration = new Configuration()

    new File(environment).listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith("xml"))
      .foreach(f => configuration.addResource(new FileInputStream(f)))

    if (configuration.get("hadoop.security.authentication") == "kerberos") {
      val props = PropertiesHelper.getKerberosProps
      val user = props.getProperty("user")
      val keytab = props.getProperty("keytab")
      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(user, keytab)
    }
    configuration
  }

}