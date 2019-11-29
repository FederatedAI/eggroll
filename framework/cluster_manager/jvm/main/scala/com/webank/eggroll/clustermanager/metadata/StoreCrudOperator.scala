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

package com.webank.eggroll.clustermanager.metadata

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.clustermanager.constant.RdbConstants
import com.webank.eggroll.core.constant.{ServerNodeStatus, NodeTypes, PartitionStatus, StoreStatus}
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.framework.clustermanager.dao.generated.mapper.{ServerNodeMapper, StoreLocatorMapper, StorePartitionMapper}
import com.webank.eggroll.framework.clustermanager.dao.generated.model._
import org.apache.ibatis.session.SqlSession

import scala.collection.mutable.ArrayBuffer

class StoreCrudOperator extends CrudOperator with Logging {
  private val crudOperatorTemplate = new CrudOperatorTemplate()
  def getOrCreateStore(input: ErStore): ErStore = {
    def doGetOrCreateStore(input: ErStore, sqlSession: SqlSession): ErStore = {
      val existing = StoreCrudOperator.doGetStore(input, sqlSession)
      if (existing != null) {
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
  private[metadata] def doGetStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputStoreLocator = input.storeLocator
    val storeLocatorExample = new StoreLocatorExample
    storeLocatorExample.createCriteria()
      .andStoreTypeEqualTo(inputStoreLocator.storeType)
      .andNamespaceEqualTo(inputStoreLocator.namespace)
      .andNameEqualTo(inputStoreLocator.name)
      .andStatusEqualTo(StoreStatus.NORMAL)
    val storeLocatorMapper = sqlSession.getMapper(classOf[StoreLocatorMapper])

    val storeLocatorResult = storeLocatorMapper.selectByExampleWithRowbounds(storeLocatorExample, RdbConstants.SINGLE_ROWBOUND)

    if (storeLocatorResult.isEmpty) {
      return null
    }

    val store = storeLocatorResult.get(0)
    val storeLocatorId = store.getStoreLocatorId

    val storePartitionExample = new StorePartitionExample
    storePartitionExample.createCriteria()
      .andStoreLocatorIdEqualTo(storeLocatorId)

    val storePartitionMapper = sqlSession.getMapper(classOf[StorePartitionMapper])
    val storePartitionResult = storePartitionMapper.selectByExample(storePartitionExample)

    if (storeLocatorResult.isEmpty) {
      return null
    }

    val nodeIdSet = new util.HashSet[java.lang.Long]()

    storePartitionResult.forEach(r => nodeIdSet.add(r.getNodeId))

    val nodeIdList = new util.ArrayList[java.lang.Long](nodeIdSet)

    val nodeExample = new ServerNodeExample
    nodeExample.createCriteria()
      .andServerNodeIdIn(nodeIdList)
      .andNodeTypeEqualTo(NodeTypes.NODE_MANAGER)
      .andStatusEqualTo(ServerNodeStatus.HEALTHY)

    val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])
    val nodeResult = nodeMapper.selectByExample(nodeExample)

    if (nodeResult.isEmpty) {
      return null
    }

    val nodeIdToNode = new util.HashMap[Long, ServerNode]()
    nodeResult.forEach(n => nodeIdToNode.put(n.getServerNodeId, n))

    val outputStoreLocator = ErStoreLocator(
      storeType = store.getStoreType,
      namespace = store.getNamespace,
      name = store.getName,
      path = store.getPath,
      totalPartitions = store.getTotalPartitions,
      partitioner = store.getPartitioner,
      serdes = store.getSerdes
    )

    val outputPartitions = ArrayBuffer[ErPartition]()

    storePartitionResult.forEach(p => {
      val nodeId = p.getNodeId
      val node: ServerNode = nodeIdToNode.get(nodeId)
      val commandEndpoint = ErEndpoint(host = node.getHost, port = 20001)
      val outputPartition = ErPartition(
        id = p.getPartitionId,
        storeLocator = outputStoreLocator,
        processor = ErProcessor(id = nodeId, commandEndpoint = commandEndpoint, dataEndpoint = commandEndpoint.copy(port = 20001)))
      outputPartitions += outputPartition
    })

    ErStore(storeLocator = outputStoreLocator, partitions = outputPartitions.toArray)
  }

  private[metadata] def doCreateStore(input: ErStore, sqlSession: SqlSession): ErStore = {
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

    // todo: integrate with session mechanism
    val serverNodes = ServerNodeCrudOperator.doGetServerNodes(
        input = ErServerNode(nodeType = NodeTypes.NODE_MANAGER, status = ServerNodeStatus.HEALTHY),
      sqlSession = sqlSession)

    val healthyNodes = new ConcurrentHashMap[Long, ErServerNode]()
    serverNodes.foreach(n => healthyNodes.putIfAbsent(n.id, n))

    val newPartitions: ArrayBuffer[ErPartition] = ArrayBuffer[ErPartition]()
    newPartitions.sizeHint(inputStoreLocator.totalPartitions)

    val nodesCount = serverNodes.length
    val specifiedPartitions = input.partitions
    val isPartitionsSpecified = specifiedPartitions.length > 0
    if (isPartitionsSpecified && specifiedPartitions.length != inputStoreLocator.totalPartitions) {
      throw new IllegalArgumentException("total partitions != specifiedPartitions.length")
    }

    val storePartitionMapper = sqlSession.getMapper(classOf[StorePartitionMapper])
    for (i <- 0 until inputStoreLocator.totalPartitions) {
      val storePartitionRecord = new StorePartition
      val node: ErServerNode = if (isPartitionsSpecified && healthyNodes.contains(specifiedPartitions(i).id)) {
        healthyNodes.get(specifiedPartitions(i).id)
      } else {
        serverNodes(i % nodesCount)
      }

      storePartitionRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId)
      storePartitionRecord.setNodeId(node.id)
      storePartitionRecord.setPartitionId(i)
      storePartitionRecord.setStatus(PartitionStatus.PRIMARY)

      rowsAffected = storePartitionMapper.insertSelective(storePartitionRecord)
      if (rowsAffected != 1) {
        throw new CrudException(s"Illegal rows affected returned when creating store partition: ${rowsAffected}")
      }

      // todo: bind with active session
      newPartitions += ErPartition(id = i,
        storeLocator = inputStoreLocator,
        processor = ErProcessor(id = storePartitionRecord.getNodeId,
          commandEndpoint = node.endpoint.copy(port = 20001),
          dataEndpoint = node.endpoint.copy(port = 20001)))
    }

    ErStore(storeLocator = inputStoreLocator, partitions = newPartitions.toArray)
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

    val storeResult = storeMapper.selectByExampleWithRowbounds(storeLocatorExample, RdbConstants.SINGLE_ROWBOUND)

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
