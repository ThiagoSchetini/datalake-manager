package br.com.bvs.datalake.model.property

import br.com.bvs.datalake.model.serializable.SerializableToCSV

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.control.Breaks.{break, breakable}

trait TransactionProps extends SerializableToCSV {
  val transactionName: String

  def validateMyself: Boolean = {
    val rm = scala.reflect.runtime.currentMirror

    val accessors = rm.classSymbol(this.getClass).toType.members.collect {
      case m: MethodSymbol if m.isGetter && m.isPublic => m
    }

    val instanceMirror = rm.reflect(this)
    var flag = true

    breakable {
      for (a <- accessors) {
        val value = instanceMirror.reflectMethod(a).apply()

        value match {

          /* no empty values inside a list */
          case v: List[Any] =>
            if (v.nonEmpty)
              v.foreach(i =>
                if (i.asInstanceOf[String].isEmpty) {
                  flag = false
                  break
                }
              )

          /* no empty values at all */
          case _ =>
            if (value == null || value.toString.isEmpty) {
              flag = false
              break
            }
        }
      }
    }

    flag
  }

  private def serializeList(list: List[String]): String = {
    val builder = new StringBuilder()
    val last = list.last
    val noLast = list.reverse.tail.reverse
    noLast.foreach(i => builder.append(s"$i,"))
    builder.append(last)
    builder.mkString
  }

  def serializeToCSV: StringBuilder = {
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
        case v: List[Any] => unorderedFields += a.name.decodedName.toString -> serializeList(v.asInstanceOf[List[String]])
        case v: Any => unorderedFields += a.name.decodedName.toString -> v.toString
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
