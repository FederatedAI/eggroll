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

import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.SessionMetaDao
import com.webank.eggroll.core.resourcemanager.ResourceDao
import com.webank.eggroll.core.util.{Logging, TimeUtils}
import org.apache.commons.lang3.StringUtils
import com.webank.eggroll.core.util.JdbcTemplate.ResultSetIterator

import scala.collection.mutable.ArrayBuffer

class StoreCrudOperator extends CrudOperator with Logging {

  def getOrCreateStore(input: ErStore): ErStore = synchronized {
    def doGetOrCreateStore(input: ErStore): ErStore = {
      val inputStoreLocator = input.storeLocator
      val inputWithoutType = input.copy(storeLocator = inputStoreLocator.copy(storeType = StringConstants.EMPTY))
      val inputStoreType = inputStoreLocator.storeType
      val existing = StoreCrudOperator.doGetStore(inputWithoutType)
      if (existing != null) {
        if (!existing.storeLocator.storeType.equals(inputStoreType)) {
          logWarning(
            s"store namespace: ${inputStoreLocator.namespace}, name: ${inputStoreLocator.name} " +
              s"already exist with store type: ${existing.storeLocator.storeType}. " +
              s"requires type: ${inputStoreLocator.storeType}")
        }
        existing
      } else {
        StoreCrudOperator.doCreateStore(input)
      }
    }

    doGetOrCreateStore(input)
  }

  def getStore(input: ErStore): ErStore = {
    StoreCrudOperator.doGetStore(input)
  }

  def deleteStore(input: ErStore): ErStore = {
    StoreCrudOperator.doDeleteStore(input)
  }

  def getStoreFromNamespace(input: ErStore): ErStoreList = {
    StoreCrudOperator.dao.getStoreLocators(input: ErStore)
  }
}

object StoreCrudOperator {
  private lazy val dbc = ResourceDao.dbc
  private val nodeIdToNode = new ConcurrentHashMap[java.lang.Long, DbServerNode]()
  private[metadata] def doGetStore(input: ErStore): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

    // getting input locator
    val inputStoreLocator = input.storeLocator
    var queryStoreLocator = "select * from store_locator " +
      "where namespace = ? and name = ? and status = ?"
    var params = List(inputStoreLocator.namespace, inputStoreLocator.name, StoreStatus.NORMAL)

    if (!StringUtils.isBlank(inputStoreLocator.storeType)) {
      queryStoreLocator += " and store_type = ?"
      params ++= Array(inputStoreLocator.storeType)
    }

    queryStoreLocator += " limit 1"

    val storeLocatorResult = dbc.query(rs =>
      rs.map(_ => DbStoreLocator(
        id = rs.getLong("store_locator_id"),
        storeType = rs.getString("store_type"),
        namespace = rs.getString("namespace"),
        name = rs.getString("name"),
        path = rs.getString("path"),
        totalPartitions = rs.getInt("total_partitions"),
        partitioner = rs.getString("partitioner"),
        serdes = rs.getString("serdes")
      )), queryStoreLocator, params:_*).toList

    if (storeLocatorResult.isEmpty) {
      return null
    }

    val store = storeLocatorResult(0)
    val storeLocatorId = store.id

    val queryStorePartition = "select * from store_partition " +
      "where store_locator_id = ? order by store_partition_id asc"

    val storePartitionResult = dbc.query(rs => rs.map(_ => DbStorePartition(
      nodeId = rs.getLong("node_id"),
      partitionId = rs.getInt("partition_id"))), queryStorePartition, storeLocatorId).toList

    if (storePartitionResult.isEmpty) {
      throw new IllegalStateException("store locator found but no partition found")
    }

    val missingNodeId = ArrayBuffer[Long]()
    val partitionAtNodeIds = ArrayBuffer[Long]()

    for (i <- 0 until storePartitionResult.size){
      val nodeId = storePartitionResult(i).nodeId
      if (!nodeIdToNode.containsKey(nodeId)) missingNodeId += nodeId
      partitionAtNodeIds += nodeId
    }

    if (!missingNodeId.isEmpty) {
      var first = true
      val queryServerNode = new StringBuilder()
      queryServerNode.append(s"select * from server_node where status = '${ServerNodeStatus.HEALTHY}'")
        .append(s" and node_type = '${ServerNodeTypes.NODE_MANAGER}'")
        .append(s" and server_node_id in (")
      for(i <- 0 until missingNodeId.length){
        if (first) first = false else queryServerNode.append(", ")
        queryServerNode.append("?")
      }
      queryServerNode.append(")")

      val nodeResult = dbc.query(rs => rs.map(_ => DbServerNode(
        id = rs.getLong("server_node_id"),
        name = rs.getString("name"),
        clusterId = rs.getLong("server_cluster_id"),
        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")),
        nodeType = rs.getString("node_type"),
        status = rs.getString("status"),
        lastHeartbeatAt = rs.getDate("last_heartbeat_at"),
        createdAt = rs.getDate("created_at"),
        updatedAt = rs.getDate("updated_at")
      )),
        queryServerNode.toString(),
        missingNodeId:_*).toList

      if (nodeResult.isEmpty) {
        throw new IllegalStateException(s"No valid node for this store: ${inputStoreLocator}")
      }

      for (i <- 0 until nodeResult.length){
        val serverNodeId = nodeResult(i).id
        nodeIdToNode.putIfAbsent(serverNodeId, nodeResult(i))
      }
    }

    val outputStoreLocator = ErStoreLocator(
      storeType = store.storeType,
      namespace = store.namespace,
      name = store.name,
      path = store.path,
      totalPartitions = store.totalPartitions,
      partitioner = store.partitioner,
      serdes = store.serdes)

    val outputOptions = new ConcurrentHashMap[String, String]()
    if (inputOptions != null) {
      outputOptions.putAll(inputOptions)
    }

    // process output partitions
    val outputPartitions = storePartitionResult.map(p => ErPartition(
        id = p.partitionId,
        storeLocator = outputStoreLocator,
        processor = ErProcessor(id = p.partitionId.toLong, serverNodeId = p.nodeId)))

    ErStore(storeLocator = outputStoreLocator, partitions = outputPartitions.toArray, options = outputOptions)
  }

  private[metadata] def doCreateStore(input: ErStore): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

    // create store locator
    val inputStoreLocator = input.storeLocator

    val newStoreLocator = dbc.withTransaction(conn => {
      val sql = "insert into store_locator " +
        "(store_type, namespace, name, path, total_partitions, " +
        "partitioner, serdes, status) values (?, ?, ?, ?, ?, ?, ?, ?)"
      dbc.update(conn, sql,
        inputStoreLocator.storeType,
        inputStoreLocator.namespace,
        inputStoreLocator.name,
        inputStoreLocator.path,
        inputStoreLocator.totalPartitions,
        inputStoreLocator.partitioner,
        inputStoreLocator.serdes,
        StoreStatus.NORMAL)
    })

    if (newStoreLocator.isEmpty){
      throw new CrudException(s"Illegal rows affected returned when creating store locator: 0")
    }

    // create partitions
    var newTotalPartitions = inputStoreLocator.totalPartitions

    val newPartitions: ArrayBuffer[ErPartition] = ArrayBuffer[ErPartition]()
    newPartitions.sizeHint(inputStoreLocator.totalPartitions)

    val serverNodes: Array[ErServerNode] =
      ServerNodeCrudOperator.doGetServerNodes(
        input = ErServerNode(
          nodeType = ServerNodeTypes.NODE_MANAGER,
          status = ServerNodeStatus.HEALTHY))

    val nodesCount = serverNodes.length
    val specifiedPartitions = input.partitions
    val isPartitionsSpecified = specifiedPartitions.length > 0

    if (newTotalPartitions <= 0) newTotalPartitions = nodesCount << 2

    val serverNodeIds = ArrayBuffer[Long]()
    for (i <- 0 until newTotalPartitions) {

      val node: ErServerNode = serverNodes(i % nodesCount)

      val nodeRecord = dbc.withTransaction( conn => {
        val sql = "insert into store_partition (store_locator_id, node_id, partition_id, status) values (?, ?, ?, ?)"

        dbc.update(conn, sql,
          newStoreLocator.get,
          if (isPartitionsSpecified) input.partitions(i).processor.serverNodeId else node.id,
          i,
          PartitionStatus.PRIMARY)
      })

      if (nodeRecord.isEmpty) {
        throw new CrudException(s"Illegal rows affected when creating node: 0")
      }

      serverNodeIds += node.id
      newPartitions += ErPartition(
        id = i,
        storeLocator = inputStoreLocator,
        processor = ErProcessor(id = i,
          serverNodeId = if (isPartitionsSpecified) input.partitions(i).processor.serverNodeId else node.id,
          tag = "binding"))
    }

    val newOptions = new ConcurrentHashMap[String, String]()
    if (inputOptions != null) newOptions.putAll(inputOptions)
    val result = ErStore(
      storeLocator = inputStoreLocator,
      partitions = newPartitions.toArray,
      options = newOptions)

    result
  }

  private[metadata] def doDeleteStore(input: ErStore): ErStore = {
    val inputStoreLocator = input.storeLocator

    val sql = "select * from store_locator " +
      "where store_type = ? and namespace = ? and name = ? and status = ? limit 1"

    val nodeResult = dbc.query(rs => rs.map(_ => DbStoreLocator(
      id = rs.getLong("store_locator_id"),
      name = rs.getString("name"))), sql,
      inputStoreLocator.storeType, inputStoreLocator.namespace,
      inputStoreLocator.name, StoreStatus.NORMAL).toList

    if (nodeResult.isEmpty) {
      return null
    }

    val nodeRecord = nodeResult(0)
    val nameNow = nodeRecord.name + "." + TimeUtils.getNowMs()

    val storeLocatorRecord = dbc.withTransaction(conn => {
      val sql = "update store_locator " +
        "set name = ?, status = ? where store_locator_id = ?"

      dbc.update(conn, sql, nameNow, StoreStatus.DELETED, nodeRecord.id)
    })

    val outputStoreLocator = inputStoreLocator.copy(name = nodeRecord.name)

    ErStore(storeLocator = outputStoreLocator)
  }
  val dao = new SessionMetaDao()
}
