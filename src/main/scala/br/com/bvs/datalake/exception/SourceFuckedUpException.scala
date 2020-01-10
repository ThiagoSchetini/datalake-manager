package br.com.bvs.datalake.exception

class SourceFuckedUpException(message: String) extends Exception {

  override def getMessage(): String = {
    message
  }
}
