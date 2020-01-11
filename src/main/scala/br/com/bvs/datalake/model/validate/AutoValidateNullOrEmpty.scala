package br.com.bvs.datalake.model.validate

import scala.reflect.runtime.universe._
import scala.util.control.Breaks.{break, breakable}

trait AutoValidateNullOrEmpty {

  def autoValidateNullOrEmpty: Boolean = {
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
}
