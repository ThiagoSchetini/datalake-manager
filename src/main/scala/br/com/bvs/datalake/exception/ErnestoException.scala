package br.com.bvs.datalake.exception

class ErnestoException(message: String) extends Exception {

  override def getMessage(): String = {
    message
  }
}
