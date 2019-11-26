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
import java.util.Date

import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta.{ErEndpoint, ErServerCluster, ErServerNode}
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.framework.clustermanager.dao.generated.mapper.ServerNodeMapper
import com.webank.eggroll.framework.clustermanager.dao.generated.model.{ServerNode, ServerNodeExample}
import org.apache.commons.lang3.StringUtils
import org.apache.ibatis.session.SqlSession

import scala.collection.mutable.ArrayBuffer

class ServerNodeCrudOperator extends CrudOperator with Logging {
  private val crudOperatorTemplate = new CrudOperatorTemplate()
  def getServerCluster(input: ErServerCluster): ErServerCluster = {
    crudOperatorTemplate.doCrudOperationSingleResult(ServerNodeCrudOperator.doGetServerCluster, input)
  }

  def getServerNode(input: ErServerNode): ErServerNode = {
    val nodeResult = crudOperatorTemplate.doCrudOperationMultipleResults(ServerNodeCrudOperator.doGetServerNodes, input)

    if (nodeResult.nonEmpty) {
      nodeResult(0)
    } else {
      null
    }
  }

  def getOrCreateServerNode(input: ErServerNode): ErServerNode = {
    crudOperatorTemplate.doCrudOperationSingleResult(functor = ServerNodeCrudOperator.doGetOrCreateServerNode, input = input, openTransaction = true)
  }

  def createOrUpdateServerNode(input: ErServerNode): ErServerNode = {
    def samFunctor(input: ErServerNode, sqlSession: SqlSession): ErServerNode = {
      ServerNodeCrudOperator.doCreateOrUpdateServerNode(input = input, sqlSession = sqlSession, isHeartbeat = false)
    }

    crudOperatorTemplate.doCrudOperationSingleResult(functor = samFunctor, input = input, openTransaction = true)
  }

  def getServerNodes(input: ErServerNode): ErServerCluster = {
    val serverNodes = crudOperatorTemplate.doCrudOperationMultipleResults(ServerNodeCrudOperator.doGetServerNodes, input)

    if (serverNodes.nonEmpty) {
      ErServerCluster(id = 0, serverNodes = serverNodes)
    } else {
      null
    }
  }
}

object ServerNodeCrudOperator {
  private[metadata] def doGetServerCluster(input: ErServerCluster, sqlSession: SqlSession): ErServerCluster = {
    val nodeExample = new ServerNodeExample
    nodeExample.createCriteria()
      .andServerClusterIdEqualTo(input.id)

    val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])

    val nodeResult = nodeMapper.selectByExample(nodeExample)
    if (nodeResult.isEmpty) {
      return null
    }

    val nodes = ArrayBuffer[ErServerNode]()

    nodeResult.forEach(n => {
      nodes += ErServerNode(id = n.getServerNodeId, name = n.getName, endpoint = ErEndpoint(host = n.getHost, port = n.getPort))
    })

    input.copy(serverNodes = nodes.toArray)
  }

  private[metadata] def doCreateServerNode(input: ErServerNode, sqlSession: SqlSession): ErServerNode = {
    val nodeRecord = new ServerNode
    nodeRecord.setName(input.name)
    nodeRecord.setServerClusterId(input.clusterId)
    nodeRecord.setHost(input.endpoint.host)
    nodeRecord.setPort(input.endpoint.port)
    nodeRecord.setNodeType(input.nodeType)
    nodeRecord.setStatus(input.status)

    val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])
    val rowsAffected = nodeMapper.insertSelective(nodeRecord)

    if (rowsAffected != 1) {
      throw new CrudException(s"Illegal rows affected when creating node: ${rowsAffected}")
    }

    ErServerNode(
      id = nodeRecord.getServerNodeId,
      name = nodeRecord.getName,
      clusterId = nodeRecord.getServerClusterId,
      endpoint = ErEndpoint(host = nodeRecord.getHost, port = nodeRecord.getPort),
      nodeType = nodeRecord.getNodeType,
      status = nodeRecord.getStatus)
  }

  private[metadata] def doUpdateServerNode(input: ErServerNode, sqlSession: SqlSession, isHeartbeat: Boolean = false): ErServerNode = {
    val nodeRecord = new ServerNode
    nodeRecord.setServerNodeId(input.id)
    nodeRecord.setName(input.name)
    nodeRecord.setServerClusterId(input.clusterId)
    nodeRecord.setHost(input.endpoint.host)
    nodeRecord.setPort(input.endpoint.port)
    nodeRecord.setNodeType(input.nodeType)
    nodeRecord.setStatus(input.status)
    if (isHeartbeat) nodeRecord.setLastHeartbeatAt(new Date())

    val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])
    val rowsAffected = nodeMapper.updateByPrimaryKeySelective(nodeRecord)

    if (rowsAffected != 1) {
      throw new CrudException(s"Illegal rows affected when updating node: ${rowsAffected}")
    }

    ErServerNode(
      id = nodeRecord.getServerNodeId,
      name = nodeRecord.getName,
      clusterId = nodeRecord.getServerClusterId,
      endpoint = ErEndpoint(host = nodeRecord.getHost, port = nodeRecord.getPort),
      nodeType = nodeRecord.getNodeType,
      status = nodeRecord.getStatus)
  }

  private[metadata] def doGetServerNodes(input: ErServerNode, sqlSession: SqlSession): Array[ErServerNode] = {
    val nodeExample = new ServerNodeExample
    val criteria = nodeExample.createCriteria()
    if (input.id > 0) criteria.andServerNodeIdEqualTo(input.id)
    if (!StringUtils.isBlank(input.name)) criteria.andNameEqualTo(input.name)
    if (input.clusterId >= 0) criteria.andServerClusterIdEqualTo(input.clusterId)
    if (!StringUtils.isBlank(input.endpoint.host)) criteria.andHostEqualTo(input.endpoint.host)
    if (input.endpoint.port > 0) criteria.andPortEqualTo(input.endpoint.port)
    if (!StringUtils.isBlank(input.nodeType)) criteria.andNodeTypeEqualTo(input.nodeType)
    if (!StringUtils.isBlank(input.status)) criteria.andStatusEqualTo(input.status)

    var nodeResult: util.List[ServerNode] = new util.ArrayList[ServerNode]()
    if (criteria.isValid && !criteria.getAllCriteria.isEmpty) {
      val nodeMapper = sqlSession.getMapper(classOf[ServerNodeMapper])
      nodeResult = nodeMapper.selectByExample(nodeExample)
    }

    val resultBuffer = ArrayBuffer[ErServerNode]()
    resultBuffer.sizeHint(nodeResult.size())
    nodeResult.forEach(n => {
      val erNode = ErServerNode(
        id = n.getServerNodeId,
        name = n.getName,
        clusterId = n.getServerClusterId,
        endpoint = ErEndpoint(host = n.getHost, port = n.getPort),
        nodeType = n.getNodeType,
        status = n.getStatus)

      resultBuffer += erNode
    })

    resultBuffer.toArray
  }

  def doGetOrCreateServerNode(input: ErServerNode, sqlSession: SqlSession): ErServerNode = {
    val existing = ServerNodeCrudOperator.doGetServerNodes(input, sqlSession)

    if (existing.nonEmpty) {
      existing(0)
    } else {
      doCreateServerNode(input, sqlSession)
    }
  }

  def doCreateOrUpdateServerNode(input: ErServerNode, sqlSession: SqlSession, isHeartbeat: Boolean = false): ErServerNode = {
    val existing = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(id = input.id), sqlSession)
    if (existing.nonEmpty) {
      doUpdateServerNode(input, sqlSession, isHeartbeat)
    } else {
      doCreateServerNode(input, sqlSession)
    }
  }
}
