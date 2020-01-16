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
import java.{lang, util}

import com.webank.eggroll.core.clustermanager.dao.generated.mapper.{ServerNodeMapper, StoreLocatorMapper, StorePartitionMapper}
import com.webank.eggroll.core.clustermanager.dao.generated.model._
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.ibatis.session.{RowBounds, SqlSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class StoreCrudOperator extends CrudOperator with Logging {
  private val crudOperatorTemplate = new CrudOperatorTemplate()
  def getOrCreateStore(input: ErStore): ErStore = synchronized {
    def doGetOrCreateStore(input: ErStore, sqlSession: SqlSession): ErStore = {
      val inputStoreLocator = input.storeLocator
      val inputWithoutType = input.copy(storeLocator = inputStoreLocator.copy(storeType = StringConstants.EMPTY))
      val inputStoreType = inputStoreLocator.storeType
      val existing = StoreCrudOperator.doGetStore(inputWithoutType, sqlSession)
      if (existing != null) {
        if (!existing.storeLocator.storeType.equals(inputStoreType)) {
          logWarning(
            s"store namespace: ${inputStoreLocator.namespace}, name: ${inputStoreLocator.name} " +
              s"already exist with store type: ${existing.storeLocator.storeType}. " +
              s"requires type: ${inputStoreLocator.storeType}")
        }
        existing
      } else {
        StoreCrudOperator.doCreateStore(input, sqlSession)
      }
    }

    crudOperatorTemplate.doCrudOperationSingleResult(doGetOrCreateStore, input, openTransaction = true)
  }

  def getStore(input: ErStore): ErStore = {
    crudOperatorTemplate.doCrudOperationSingleResult(StoreCrudOperator.doGetStore, input)
  }

  def deleteStore(input: ErStore): ErStore = {
    crudOperatorTemplate.doCrudOperationSingleResult(StoreCrudOperator.doDeleteStore, input, openTransaction = true)
  }
}

object StoreCrudOperator {
  private val nodeIdToNode = new ConcurrentHashMap[java.lang.Long, ServerNode]()
  private[metadata] def doGetStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

    // getting input locator
    val inputStoreLocator = input.storeLocator
    val storeLocatorExample = new StoreLocatorExample
    val criteria = storeLocatorExample.createCriteria()
      .andNamespaceEqualTo(inputStoreLocator.namespace)
      .andNameEqualTo(inputStoreLocator.name)
      .andStatusEqualTo(StoreStatus.NORMAL)

    if (!StringUtils.isBlank(inputStoreLocator.storeType)) {
      criteria.andStoreTypeEqualTo(inputStoreLocator.storeType)
    }

    val storeLocatorMapper = sqlSession.getMapper(classOf[StoreLocatorMapper])

    val storeLocatorResult = storeLocatorMapper.selectByExampleWithRowbounds(storeLocatorExample, new RowBounds(0, 1))

    if (storeLocatorResult.isEmpty) {
      return null
    }

    val store = storeLocatorResult.get(0)
    val storeLocatorId = store.getStoreLocatorId

    // getting partitions
    val storePartitionExample = new StorePartitionExample
    storePartitionExample.createCriteria()
      .andStoreLocatorIdEqualTo(storeLocatorId)

    storeLocatorExample.setOrderByClause("store_partition_id asc")

    val storePartitionMapper = sqlSession.getMapper(classOf[StorePartitionMapper])
    val storePartitionResult = storePartitionMapper.selectByExample(storePartitionExample)

    if (storePartitionResult.isEmpty) {
      throw new IllegalStateException("store locator found but no partition found")
    }

    val missingNodeId = new util.HashSet[java.lang.Long](storePartitionResult.size())
    val partitionAtNodeIds = ArrayBuffer[Long]()

    storePartitionResult.forEach(r => {
      val nodeId = r.getNodeId
      if (!nodeIdToNode.containsKey(nodeId)) missingNodeId.add(nodeId)
      partitionAtNodeIds += nodeId
    })

    if (!missingNodeId.isEmpty) {
      val nodeExample = new ServerNodeExample
      nodeExample.createCriteria()
        .andServerNodeIdIn(new util.ArrayList[lang.Long](missingNodeId))
        .andNodeTypeEqualTo(ServerNodeTypes.NODE_MANAGER)
        .andStatusEqualTo(ServerNodeStatus.HEALTHY)

      val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])
      val nodeResult = nodeMapper.selectByExample(nodeExample)

      if (nodeResult.isEmpty) {
        throw new IllegalStateException(s"No valid node for this store: ${inputStoreLocator}")
      }

      nodeResult.forEach(n => {nodeIdToNode.putIfAbsent(n.getServerNodeId, n)})
    }

    val outputStoreLocator = ErStoreLocator(
      storeType = store.getStoreType,
      namespace = store.getNamespace,
      name = store.getName,
      path = store.getPath,
      totalPartitions = store.getTotalPartitions,
      partitioner = store.getPartitioner,
      serdes = store.getSerdes
    )

    val outputOptions = new ConcurrentHashMap[String, String]()
    if (inputOptions != null) {
      outputOptions.putAll(inputOptions)
    }

    // process output partitions
    val outputPartitions = storePartitionResult.asScala
      .map(p => ErPartition(
        id = p.getPartitionId,
        storeLocator = outputStoreLocator,
        processor = ErProcessor(id = p.getPartitionId.toLong, serverNodeId = p.getNodeId)))

    ErStore(storeLocator = outputStoreLocator, partitions = outputPartitions.toArray, options = outputOptions)
  }

  private[metadata] def doCreateStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

    // create store locator
    val inputStoreLocator = input.storeLocator
    val newStoreLocator = new StoreLocator

    newStoreLocator.setStoreType(inputStoreLocator.storeType)
    newStoreLocator.setNamespace(inputStoreLocator.namespace)
    newStoreLocator.setName(inputStoreLocator.name)
    newStoreLocator.setPath(inputStoreLocator.path)
    newStoreLocator.setTotalPartitions(inputStoreLocator.totalPartitions)
    newStoreLocator.setPartitioner(inputStoreLocator.partitioner)
    newStoreLocator.setSerdes(inputStoreLocator.serdes)
    newStoreLocator.setStatus(StoreStatus.NORMAL)

    val storeLocatorMapper = sqlSession.getMapper(classOf[StoreLocatorMapper])
    var rowsAffected = storeLocatorMapper.insertSelective(newStoreLocator)
    if (rowsAffected != 1) {
      throw new CrudException(s"Illegal rows affected returned when creating store locator: ${rowsAffected}")
    }

    // create partitions
    var newTotalPartitions = inputStoreLocator.totalPartitions

    val newPartitions: ArrayBuffer[ErPartition] = ArrayBuffer[ErPartition]()
    newPartitions.sizeHint(inputStoreLocator.totalPartitions)

    val serverNodes: Array[ErServerNode] =
      ServerNodeCrudOperator.doGetServerNodes(
        input = ErServerNode(
          nodeType = ServerNodeTypes.NODE_MANAGER,
          status = ServerNodeStatus.HEALTHY), sqlSession = sqlSession)

    val nodesCount = serverNodes.length
    val specifiedPartitions = input.partitions
    val isPartitionsSpecified = specifiedPartitions.length > 0

    if (newTotalPartitions <= 0) newTotalPartitions = nodesCount << 2
    val storePartitionMapper = sqlSession.getMapper(classOf[StorePartitionMapper])

    val serverNodeIds = ArrayBuffer[Long]()
    for (i <- 0 until newTotalPartitions) {

      val storePartitionRecord = new StorePartition
      val node: ErServerNode = serverNodes(i % nodesCount)

      storePartitionRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId)
      storePartitionRecord.setNodeId(if (isPartitionsSpecified) input.partitions(i).processor.serverNodeId else node.id)
      storePartitionRecord.setPartitionId(i)
      storePartitionRecord.setStatus(PartitionStatus.PRIMARY)

      rowsAffected = storePartitionMapper.insertSelective(storePartitionRecord)

      if (rowsAffected != 1) {
        throw new CrudException(s"Illegal rows affected returned when creating store partition: ${rowsAffected}")
      }

      serverNodeIds += node.id
      newPartitions += ErPartition(
        id = i,
        storeLocator = inputStoreLocator,
        processor = ErProcessor(id = i,
          serverNodeId = storePartitionRecord.getNodeId,
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

  private[metadata] def doDeleteStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputStoreLocator = input.storeLocator

    val storeLocatorExample = new StoreLocatorExample
    storeLocatorExample.createCriteria()
      .andStoreTypeEqualTo(inputStoreLocator.storeType)
      .andNamespaceEqualTo(inputStoreLocator.namespace)
      .andNameEqualTo(inputStoreLocator.name)
      .andStatusEqualTo(StoreStatus.NORMAL)
    val storeMapper = sqlSession.getMapper(classOf[StoreLocatorMapper])

    val storeResult = storeMapper.selectByExampleWithRowbounds(storeLocatorExample, new RowBounds(0, 1))

    if (storeResult.isEmpty) {
      return null
    }

    val now = System.currentTimeMillis()
    val store = storeResult.get(0)
    store.setName(s"${store.getName}.${now}")
    store.setStatus(StoreStatus.DELETED)

    val rowsAffected = storeMapper.updateByPrimaryKeySelective(store)

    if (rowsAffected != 1) {
      throw new CrudException(s"Illegal rows affected returned when deleting store: ${rowsAffected}")
    }

    val outputStoreLocator = inputStoreLocator.copy(name = store.getName)

    ErStore(storeLocator = outputStoreLocator)
  }
}
