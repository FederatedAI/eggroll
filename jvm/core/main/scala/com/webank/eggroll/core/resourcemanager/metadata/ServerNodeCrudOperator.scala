/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.resourcemanager.metadata

import java.util
import java.util.Date

import com.webank.eggroll.core.constant.{ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta.{ErEndpoint, ErServerCluster, ErServerNode}
import com.webank.eggroll.core.resourcemanager.ResourceDao
import com.webank.eggroll.core.util.JdbcTemplate.ResultSetIterator
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait CrudOperator

class ServerNodeCrudOperator extends CrudOperator with Logging {
  def getServerCluster(input: ErServerCluster): ErServerCluster = {
    ServerNodeCrudOperator.doGetServerCluster(input)
  }

  def getServerNode(input: ErServerNode): ErServerNode = synchronized {
    val nodeResult = ServerNodeCrudOperator.doGetServerNodes(input)

    if (nodeResult.nonEmpty) {
      nodeResult(0)
    } else {
      null
    }
  }

  def getOrCreateServerNode(input: ErServerNode): ErServerNode = synchronized {
    ServerNodeCrudOperator.doGetOrCreateServerNode(input)
  }

  def createOrUpdateServerNode(input: ErServerNode): ErServerNode = synchronized {
    def samFunctor(input: ErServerNode): ErServerNode = {
      ServerNodeCrudOperator.doCreateOrUpdateServerNode(input = input, isHeartbeat = false)
    }

    samFunctor(input)
  }

  def getServerNodes(input: ErServerNode): ErServerCluster = synchronized {
    val serverNodes = ServerNodeCrudOperator.doGetServerNodes(input)

    if (serverNodes.nonEmpty) {
      ErServerCluster(id = 0, serverNodes = serverNodes)
    } else {
      null
    }
  }

  def getServerClusterByHosts(input: util.List[String]): ErServerCluster = synchronized {
    ServerNodeCrudOperator.doGetServerClusterByHosts(input)
  }
}

object ServerNodeCrudOperator extends Logging {
  private lazy val dbc = ResourceDao.dbc
  private[metadata] def doGetServerCluster(input: ErServerCluster): ErServerCluster = {
    val sql = "select * from server_node where server_cluster_id = ?"
    val nodeResult = dbc.query(rs =>
      rs.map(_ => ErServerNode(
        id = rs.getInt("server_node_id"), 
        name = rs.getString("name"),
        endpoint = ErEndpoint(host=rs.getString("host"), port = rs.getInt("port")))
      ), sql, input.id)

    if (nodeResult.isEmpty) return null

    ErServerCluster(serverNodes = nodeResult.toArray)
  }

  private[metadata] def doCreateServerNode(input: ErServerNode): ErServerNode = {
    val nodeRecord = dbc.withTransaction(conn => {
      val sql = "insert into server_node (name, server_cluster_id, host, port, node_type, status)" +
        " values (?, ?, ?, ?, ?, ?)"
      dbc.update(conn, sql, input.name, input.clusterId,
        input.endpoint.host, input.endpoint.port, input.nodeType,input.status)
    })

    if (nodeRecord.isEmpty) {
      throw new CrudException(s"Illegal rows affected when creating node: ${nodeRecord}")
    }

    ErServerNode(
      id = nodeRecord.get,
      name = input.name,
      clusterId = input.clusterId,
      endpoint = ErEndpoint(host = input.endpoint.host, port = input.endpoint.port),
      nodeType = input.nodeType,
      status = input.status)
  }

  private[metadata] def existSession(sessionId: Long): Boolean = {
    dbc.queryOne("select * from session_main where session_id = ?", sessionId).nonEmpty
  }

  def doUpdateServerNode(input: ErServerNode, isHeartbeat: Boolean = false): ErServerNode = {
    val nodeRecord = dbc.withTransaction(conn => {
      var sql = "update server_node set " +
        "server_node_id = ?, name = ?, server_cluster_id = ?, host = ?, port = ?, node_type = ?, status = ?"
      var params = List(input.id, input.name, input.clusterId,
        input.endpoint.host, input.endpoint.port, input.nodeType, input.status)

      if (isHeartbeat) {
        sql += ", last_heartbeat_at = ?"
        params ++= Array(new Date())
      }

      dbc.update(conn, sql, params:_*)
    })

    if (nodeRecord.isEmpty) {
      throw new CrudException(s"Illegal rows affected when creating node: ${nodeRecord}")
    }

    ErServerNode(
      id = input.id,
      name = input.name,
      clusterId = input.clusterId,
      endpoint = ErEndpoint(host = input.endpoint.host, port = input.endpoint.port),
      nodeType = input.nodeType,
      status = input.status)

  }

  private[metadata] def doGetServerNodesUnwrapped(input: ErServerNode): Array[DbServerNode] = {
    var sql = "select * from server_node where 1=? "
    var params = ListBuffer("1")

    if (input.id > 0) {
      sql += "and server_node_id = ?"
      params ++= Array(input.id.toString)
    }

    if (!StringUtils.isBlank(input.name)) {
      sql += "and name = ?"
      params ++= Array(input.name)
    }

    if (input.clusterId >= 0) {
      sql += "and server_cluster_id = ?"
      params ++= Array(input.clusterId.toString)
    }

    if (!StringUtils.isBlank(input.endpoint.host)) {
      sql += "and host = ?"
      params ++= Array(input.endpoint.host)
    }

    if (input.endpoint.port > 0) {
      sql += "and port = ?"
      params ++= Array(input.endpoint.port.toString)
    }

    if (!StringUtils.isBlank(input.nodeType)) {
      sql += "and node_type = ?"
      params ++= Array(input.nodeType)
    }

    if (!StringUtils.isBlank(input.status)) {
      sql += "and status = ?"
      params ++= Array(input.status)
    }

    val nodeResult = dbc.query( rs => rs.map(_ => DbServerNode(
      id = rs.getLong("server_node_id"),
      name = rs.getString("name"),
      clusterId = rs.getLong("server_cluster_id"),
      endpoint = ErEndpoint(host=rs.getString("host"), port = rs.getInt("port")),
      nodeType = rs.getString("node_type"),
      status = rs.getString("status"),
      lastHeartbeatAt = rs.getDate("last_heartbeat_at"),
      createdAt = rs.getDate("created_at"),
      updatedAt = rs.getDate("updated_at"))), sql, params:_*).toList

    nodeResult.toArray
  }

  private[metadata] def doGetServerNodes(input: ErServerNode): Array[ErServerNode] = {
    var sql = "select * from server_node where 1=? "
    var params = List("1")

    if (input.id > 0) {
      sql += "and server_node_id = ?"
      params ++= Array(input.id.toString)
    }

    if (!StringUtils.isBlank(input.name)) {
      sql += "and name = ?"
      params ++= Array(input.name)
    }

    if (input.clusterId >= 0) {
      sql += "and server_cluster_id = ?"
      params ++= Array(input.clusterId.toString)
    }

    if (!StringUtils.isBlank(input.endpoint.host)) {
      sql += "and host = ?"
      params ++= Array(input.endpoint.host)
    }

    if (input.endpoint.port > 0) {
      sql += "and port = ?"
      params ++= Array(input.endpoint.port.toString)
    }

    if (!StringUtils.isBlank(input.nodeType)) {
      sql += "and node_type = ?"
      params ++= Array(input.nodeType)
    }

    if (!StringUtils.isBlank(input.status)) {
      sql += "and status = ?"
      params ++= Array(input.status)
    }

    sql += "order by server_node_id asc"

    val nodeResult = dbc.query(rs => rs.map(_ =>
      ErServerNode(
        id = rs.getLong("server_node_id"),
        name = rs.getString("name"),
        clusterId = rs.getLong("server_cluster_id"),
        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")),
        nodeType = rs.getString("node_type"),
        status = rs.getString("status"))), sql, params: _*)

    nodeResult.toArray

  }

  private[metadata] def doGetOrCreateServerNode(input: ErServerNode): ErServerNode = {
    val existing = ServerNodeCrudOperator.doGetServerNodes(input)

    if (existing.nonEmpty) {
      existing(0)
    } else {
      doCreateServerNode(input)
    }
  }

  def doCreateOrUpdateServerNode(input: ErServerNode, isHeartbeat: Boolean = false): ErServerNode = {
    val existing = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(id = input.id))
    if (existing.nonEmpty) {
      doUpdateServerNode(input, isHeartbeat)
    } else {
      doCreateServerNode(input)
    }
  }

  def doGetServerClusterByHosts(input: util.List[String]): ErServerCluster = {
    val inputListBuffer = input.asScala

    var first = true
    val sql = new StringBuilder()
    sql.append(s"select * from server_node where status = '${ServerNodeStatus.HEALTHY}'")
      .append(s" and node_type = '${ServerNodeTypes.NODE_MANAGER}'")
      .append(s" and host in (")
    inputListBuffer.foreach(_ => {
      if (first) first = false else sql.append(", ")
      sql.append("?")
    })
    sql.append(") order by server_node_id asc")

    val nodeResult = dbc.query(rs => rs.map(_ =>
      ErServerNode(
        id = rs.getLong("server_node_id"),
        name = rs.getString("name"),
        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")))),
      sql.toString(), inputListBuffer:_*)

    ErServerCluster(serverNodes = nodeResult.toArray)
  }
}
