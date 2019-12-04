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

package com.webank.eggroll.core.clustermanager.metadata

import java.{lang, util}
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.clustermanager.constant.RdbConstants
import com.webank.eggroll.core.constant.{BindingStrategies, PartitionStatus, ServerNodeStatus, ServerNodeTypes, SessionConfKeys, StoreStatus, StringConstants}
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.core.clustermanager.dao.generated.mapper.{ServerNodeMapper, StoreLocatorMapper, StorePartitionMapper}
import com.webank.eggroll.core.clustermanager.dao.generated.model._
import com.webank.eggroll.core.clustermanager.session.SessionManager
import org.apache.ibatis.session.SqlSession

import scala.collection.mutable
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
  private val nodeIdToNode = new ConcurrentHashMap[java.lang.Long, ServerNode]()
  private[metadata] def doGetStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

    // getting input locator
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

    val existingBinding = SessionManager.getBindingPlan(
      sessionId = sessionId,
      totalPartitions = outputStoreLocator.totalPartitions,
      serverNodeIds = partitionAtNodeIds.toArray)

    // process output partitions
    val outputPartitions = ArrayBuffer[ErPartition]()
    if (existingBinding != null
      && existingBinding.id.equals(inputOptions.get(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID))
      && !inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_INCLUDE_DETAILS, "false").toBoolean) {
      outputOptions.put(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID, existingBinding.id)
    } else {
      val bindingId = ErPartitionBindingPlan.genId(
        sessionId = sessionId,
        totalPartitions = outputStoreLocator.totalPartitions,
        serverNodeIds = partitionAtNodeIds.toArray,
        strategy = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_STRATEGY, BindingStrategies.ROUND_ROBIN))
      outputOptions.put(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID, bindingId)
      outputOptions.put(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_STRATEGY, BindingStrategies.ROUND_ROBIN)

      val newBinding = SessionManager.createBindingPlan(
        sessionId = sessionId,
        totalPartitions = outputStoreLocator.totalPartitions,
        serverNodeIds = partitionAtNodeIds.toArray)
      outputPartitions ++= newBinding.toPartitions()
    }

    ErStore(storeLocator = outputStoreLocator, partitions = outputPartitions.toArray, options = outputOptions)
  }

  private[metadata] def doCreateStore(input: ErStore, sqlSession: SqlSession): ErStore = {
    val inputOptions = input.options
    val sessionId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_ID, StringConstants.UNKNOWN)

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

    val partitionServerNodeBindingId = inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID, null)
    var outputTotalPartitions = inputStoreLocator.totalPartitions

    val finalPartitions: ArrayBuffer[ErPartition] = ArrayBuffer[ErPartition]()
    finalPartitions.sizeHint(inputStoreLocator.totalPartitions)

    // todo: find existing binding with same partition numbers
    val serverNodes: Array[ErServerNode] = if (partitionServerNodeBindingId != null) {
      val binding = SessionManager.getBindingPlan(sessionId = sessionId, bindingId = partitionServerNodeBindingId)
      val existingBinding = ArrayBuffer[ErServerNode]()
      existingBinding.sizeHint(binding.totalPartitions)
      binding.partitionToServerNodes.foreach(nid => {
        val node = nodeIdToNode.get(nid)

        // todo: conversion move to util or ErServerNode
        existingBinding += ErServerNode(
          id = node.getServerNodeId,
          name = node.getName,
          clusterId = node.getServerClusterId,
          endpoint = ErEndpoint(host = node.getHost, port = node.getPort),
          nodeType = node.getNodeType,
          status = node.getStatus)

      })
      outputTotalPartitions = binding.totalPartitions
      existingBinding.toArray
    } else if (SessionManager.getSessionDeployment(sessionId) != null && !sessionId.equals(StringConstants.UNKNOWN)) {
      SessionManager.getSessionDeployment(sessionId).serverCluster.serverNodes
    } else {
      ServerNodeCrudOperator.doGetServerNodes(input = ErServerNode(nodeType = ServerNodeTypes.NODE_MANAGER, status = ServerNodeStatus.HEALTHY),
        sqlSession = sqlSession)
    }

    val nodesCount = serverNodes.length
    val specifiedPartitions = input.partitions
    val isPartitionsSpecified = specifiedPartitions.length > 0
    if (isPartitionsSpecified) {
      throw new UnsupportedOperationException("specify partition not supported yet")
    }

    if (outputTotalPartitions <= 0) outputTotalPartitions = nodesCount << 2
    val storePartitionMapper = sqlSession.getMapper(classOf[StorePartitionMapper])

    val serverNodeIds = ArrayBuffer[Long]()
    for (i <- 0 until outputTotalPartitions) {
      val storePartitionRecord = new StorePartition
      val node: ErServerNode = serverNodes(i % nodesCount)

      storePartitionRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId)
      storePartitionRecord.setNodeId(node.id)
      storePartitionRecord.setPartitionId(i)
      storePartitionRecord.setStatus(PartitionStatus.PRIMARY)

      rowsAffected = storePartitionMapper.insertSelective(storePartitionRecord)
      if (rowsAffected != 1) {
        throw new CrudException(s"Illegal rows affected returned when creating store partition: ${rowsAffected}")
      }

      serverNodeIds += node.id
      // todo: bind with active session
      finalPartitions += ErPartition(
        id = i,
        storeLocator = inputStoreLocator,
        processor = ErProcessor(id = i,
          serverNodeId = storePartitionRecord.getNodeId,
          tag = "binding"))
    }

    val finalOptions = new ConcurrentHashMap[String, String]()
    if (inputOptions != null) finalOptions.putAll(inputOptions)
    val result = if (partitionServerNodeBindingId == null
      || inputOptions.getOrDefault(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_INCLUDE_DETAILS, "false").toBoolean) {
      val partitionToNodeIds = finalPartitions.map(p => p.processor.serverNodeId).toArray

      val binding = SessionManager.createBindingPlan(
        sessionId = sessionId,
        totalPartitions = outputTotalPartitions,
        serverNodeIds = partitionToNodeIds,
        strategy = BindingStrategies.ROUND_ROBIN)


      finalOptions.put(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID, binding.id)
      finalOptions.put(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_STRATEGY, BindingStrategies.ROUND_ROBIN)
      ErStore(
        storeLocator = inputStoreLocator,
        partitions = finalPartitions.toArray,
        options = finalOptions)
    } else {
      ErStore(
        storeLocator = inputStoreLocator,
        options = finalOptions)
    }

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
