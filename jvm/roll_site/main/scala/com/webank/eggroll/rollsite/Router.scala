package com.webank.eggroll.rollsite

import com.webank.eggroll.core.meta.ErEndpoint

import scala.io.Source
import org.json.{JSONArray, JSONObject}

object Router {
  private var routerTable: JSONObject = _

  def initOrUpdateRouterTable(path: String): Unit = {
    val source = Source.fromFile(path,"UTF-8")
    val str = source.mkString
    routerTable = new JSONObject(str).get("route_table").asInstanceOf[JSONObject]
  }

  def query(partyId: String, role: String = "default"): ErEndpoint = {
    if (routerTable == null) {
      throw new Exception("The routing table is not initialized!")
    }

    if (!routerTable.has(partyId)) {
      throw new Exception(s"The routing table not have current party:${partyId}")
    }

    val rt = routerTable.get(partyId).asInstanceOf[JSONObject]

    if (!rt.has(role) && !rt.has("default")) {
      throw new Exception(s"The routing table not have current party:${partyId}")
    }

    val curRole = if (rt.has(role)) {role} else {"default"}
    val default: JSONObject = routerTable.get(partyId).asInstanceOf[JSONObject]
      .get(curRole).asInstanceOf[JSONArray]
      .get(0).asInstanceOf[JSONObject]
    val host = default.get("ip").asInstanceOf[String]
    val port = default.get("port").asInstanceOf[Int]
    ErEndpoint(host, port)
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
  }
}
