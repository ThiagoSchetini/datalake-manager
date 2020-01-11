package br.com.bvs.datalake.model.serialize

import br.com.bvs.datalake.util.TextUtil

import scala.collection.mutable
import scala.reflect.runtime.universe._

trait AutoSerializeToCSV {

  def autoSerializeCSV: StringBuilder = {
    val rm = scala.reflect.runtime.currentMirror
    val instanceMirror = rm.reflect(this)
    val accessors = rm.classSymbol(this.getClass).toType.members.collect {
      case m: MethodSymbol if m.isGetter && m.isPublic => m
    }
    val orderedFields = this.getClass.getDeclaredFields.toList.map(_.getName)
    val unorderedFields = new mutable.HashMap[String, String]()

    /* get unordered values */
    for (a <- accessors) {
      val value = instanceMirror.reflectMethod(a).apply()

      value match {
        case v: List[Any] =>
          unorderedFields += a.name.decodedName.toString -> TextUtil.serializeList(v.asInstanceOf[List[String]])

        case v: Any =>
          unorderedFields += a.name.decodedName.toString -> v.toString
      }
    }

    /* using original class sequence order */
    val fields = orderedFields zip orderedFields.map(unorderedFields)

    /* delimited out with only values */
    val csv = new StringBuilder()
    val delimiter = "|"
    fields.foreach(f => csv.append(s"${f._2}$delimiter"))
    csv.dropRight(1)
  }
}
