package br.com.bvs.datalake.model.property

import br.com.bvs.datalake.model.serialize.AutoSerializeToCSV
import br.com.bvs.datalake.model.validate.AutoValidateNullOrEmpty

trait TransactionProps extends AutoSerializeToCSV with AutoValidateNullOrEmpty{
  val smHash: String
  val transactionName: String
}
