package com.webank.eggroll.rollsite

import java.io.{File, FileInputStream, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.util.Logging
import org.json.{JSONArray, JSONObject}
import scala.io.Source

case class QueryResult(point: ErEndpoint, isSecure: Boolean, isPolling: Boolean)

object Router extends Logging{
  @volatile private var routerTable: JSONObject = _
  @volatile private var defaultEnable: Boolean = true

  def initOrUpdateRouterTable(path: String): Unit = {
    logDebug(s"refreshing route table at path=${path}")
    val source = Source.fromFile(path, "UTF-8")
    val str = source.mkString
    val js = new JSONObject(str)
    routerTable = js.get("route_table").asInstanceOf[JSONObject]
    try {
      defaultEnable = js.get("permission").asInstanceOf[JSONObject]
        .get("default_allow").asInstanceOf[Boolean]
    } catch {
      case t: Throwable =>
        logError("get default_allow from route table failed. setting defaultEnable=true", t)
        defaultEnable = true
    } finally {
      source.close()
      logTrace(s"close route table at path=${path}")
    }
  }

  def query(partyId: String, role: String = "default"): QueryResult = {
    if (routerTable == null) {
      throw new Exception("The route table is not initialized")
    }

    if (!routerTable.has(partyId) && !routerTable.has("default")) {
      throw new Exception(s"The routing table not have current party=${partyId} and default party")
    }

    val curParty = if (routerTable.has(partyId)) {partyId} else {
      if (defaultEnable) {
        "default"
      } else {
        throw new Exception(s"The routing table not have current party=${partyId} and disable default party")
      }

    }
    val rt = routerTable.get(curParty).asInstanceOf[JSONObject]

    if (!rt.has(role) && !rt.has("default")) {
      throw new Exception(s"The routing table not have current role=${role}")
    }

    val curRole = if (rt.has(role)) {role} else {
      if (defaultEnable) {
        "default"
      } else {
        throw new Exception(s"The routing table not have current role=${role} and disable default role")
      }
    }
    val default: JSONObject = routerTable.get(curParty).asInstanceOf[JSONObject]
      .get(curRole).asInstanceOf[JSONArray]
      .get(0).asInstanceOf[JSONObject]

    var isPolling = false
    if (default.has("is_polling")) {
      if (default.get("is_polling").asInstanceOf[Boolean] || default.get("is_polling").toString == "1") {
        isPolling = true
      }
    }

    var host = ""
    var port = -1
    if (!isPolling) {
      host = default.get("ip").asInstanceOf[String]
      port = default.get("port").asInstanceOf[Int]
    }

    var isSecure = false
    if (default.has("is_secure")) {
      if (default.get("is_secure").asInstanceOf[Boolean] || default.get("is_secure").toString == "1") {
        isSecure = true
      }
    }

    QueryResult(ErEndpoint(host, port), isSecure, isPolling)
  }

  private def jsonCheck(data: String): Boolean = {
    try {
      val js = new JSONObject(data)
      js.has("route_table")
    } catch {
      case t: Throwable =>
        logError("route table data check failed", t)
        false
    }
  }

  def update(jsonString: String, path: String): Unit = {
    try {
      if (jsonCheck(jsonString)) {
        val file = new File(path)
        if (!file.getParentFile.exists) file.getParentFile.mkdirs
        if (!file.exists) file.createNewFile
        val write = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)
        write.write(jsonString)
        write.flush()
        write.close()
      }
    } catch {
      case e: Throwable =>
        logError("route table update failed", e)
        throw e
    } finally {
      initOrUpdateRouterTable(path)
    }
  }

  def get(path: String): String = {
    try {
      val jsonFile = new File(path)
      val fileLength = jsonFile.length
      val fileContent = new Array[Byte](fileLength.intValue)
      val in = new FileInputStream(jsonFile)
      in.read(fileContent)
      new String(fileContent, StandardCharsets.UTF_8)
    } catch {
      case e: Throwable =>
        logError("route table get failed", e)
        throw e
    }
  }

  def main(args: Array[String]): Unit = {
    Router.initOrUpdateRouterTable("conf\\route_table.json")
    var ret = Router.query("10001", "fate_flow").point
    println(ret.getHost, ret.getPort)

    ret = Router.query("10001").point
    println(ret.getHost, ret.getPort)

    ret = Router.query("10001", "acd").point
    println(ret.getHost, ret.getPort)

    ret = Router.query("10003").point
    println(ret.getHost, ret.getPort)


    val str = Router.get("conf\\route_table.json")
    println(str)

    Router.update("{testing}", "conf\\route_table.json")
    println(Router.get("conf\\route_table.json"))

    Router.update(str, "conf\\route_table.json")
  }
}
