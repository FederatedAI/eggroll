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

  def query(partyId: String): ErEndpoint = {
    if (routerTable == null) {
      throw new Exception("The routing table is not initialized!")
    }
    val default: JSONObject = routerTable.get(partyId).asInstanceOf[JSONObject]
      .get("default").asInstanceOf[JSONArray]
      .get(0).asInstanceOf[JSONObject]
    val host = default.get("ip").asInstanceOf[String]
    val port = default.get("port").asInstanceOf[Int]
    ErEndpoint(host, port)
  }

  def main(args: Array[String]): Unit = {
    Router.initOrUpdateRouterTable("conf\\route_table.json")
    val ret = Router.query("10001")
    println(ret.getHost, ret.getPort)
  }
}
