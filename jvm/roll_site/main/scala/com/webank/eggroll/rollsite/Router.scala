package com.webank.eggroll.rollsite

import java.io.{File, FileInputStream, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import com.webank.eggroll.core.meta.ErEndpoint
import org.json.{JSONArray, JSONObject}

import scala.io.Source

object Router {
  @volatile private var routerTable: JSONObject = _
  @volatile private var defaultEnable: Boolean = true

  def initOrUpdateRouterTable(path: String): Unit = {
    val source = Source.fromFile(path,"UTF-8")
    val str = source.mkString
    val js = new JSONObject(str)
    routerTable = js.get("route_table").asInstanceOf[JSONObject]
    try {
      defaultEnable = js.get("permission").asInstanceOf[JSONObject]
        .get("default_allow").asInstanceOf[Boolean]
    } catch {
      case _: Throwable => defaultEnable = true
    }

  }

  def query(partyId: String, role: String = "default"): ErEndpoint = {
    if (routerTable == null) {
      throw new Exception("The routing table is not initialized!")
    }

    if (!routerTable.has(partyId) && !routerTable.has("default")) {
      throw new Exception(s"The routing table not have current party=${partyId} and default party.")
    }

    val curParty = if (routerTable.has(partyId)) {partyId} else {
      if (defaultEnable) {
        "default"
      } else {
        throw new Exception(s"The routing table not have current party=${partyId} and disable default party.")
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
        throw new Exception(s"The routing table not have current role=${role} and disable default role.")
      }
    }
    val default: JSONObject = routerTable.get(curParty).asInstanceOf[JSONObject]
      .get(curRole).asInstanceOf[JSONArray]
      .get(0).asInstanceOf[JSONObject]
    val host = default.get("ip").asInstanceOf[String]
    val port = default.get("port").asInstanceOf[Int]
    ErEndpoint(host, port)
  }

  def update(jsonString: String, path: String): Unit = {
    val file = new File(path)
    if (!file.getParentFile.exists) file.getParentFile.mkdirs
    if (!file.exists) file.createNewFile
    val write = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)
    write.write(jsonString)
    write.flush()
    write.close()
  }

  def get(path: String): String = {
    val jsonFile = new File(path)
    val fileLength = jsonFile.length
    val fileContent = new Array[Byte](fileLength.intValue)
    val in = new FileInputStream(jsonFile)
    in.read(fileContent)
    new String(fileContent, StandardCharsets.UTF_8)
  }

  def main(args: Array[String]): Unit = {
    Router.initOrUpdateRouterTable("conf\\route_table.json")
    var ret = Router.query("10001", "fate_flow")
    println(ret.getHost, ret.getPort)

    ret = Router.query("10001")
    println(ret.getHost, ret.getPort)

    ret = Router.query("10001", "acd")
    println(ret.getHost, ret.getPort)

    ret = Router.query("10003")
    println(ret.getHost, ret.getPort)


    val str = Router.get("conf\\route_table.json")
    println(str)

    Router.update("{testing}", "conf\\route_table.json")
    println(Router.get("conf\\route_table.json"))

    Router.update(str, "conf\\route_table.json")
  }
}
