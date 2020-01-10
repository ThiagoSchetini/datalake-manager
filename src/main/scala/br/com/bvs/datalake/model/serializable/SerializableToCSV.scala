package br.com.bvs.datalake.model.serializable

trait SerializableToCSV {
  def serializeToCSV: StringBuilder
}
