package com.webank.eggroll.util

object SchemaUtil {
  val oneDoubleFieldSchema:String =
    """{"fields": [{"name":"double0", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}]}"""

  def getDoubleSchema(fieldCount: Int): String = {
    val sb = new StringBuilder
    sb.append(
      """{
                 "fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }

  def getLongSchema(fieldCount: Int): String = {
    val sb = new StringBuilder
    sb.append(
      """{
                 "fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"long$i", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}""")
    }
    sb.append("]}")
    sb.toString()
  }
}
