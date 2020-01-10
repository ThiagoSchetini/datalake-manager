package br.com.bvs.datalake.exception

class SmartContractException(message: String) extends Exception {

  override def getMessage: String = {
    message
  }
}
