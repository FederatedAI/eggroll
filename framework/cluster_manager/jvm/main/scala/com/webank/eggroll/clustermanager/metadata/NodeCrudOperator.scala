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

import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta.{ErEndpoint, ErServerNode, ErServerCluster, ErProcessor}
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.framework.clustermanager.dao.generated.mapper.ServerNodeMapper
import com.webank.eggroll.framework.clustermanager.dao.generated.model.{ServerNode, ServerNodeExample}
import org.apache.commons.lang3.StringUtils
import org.apache.ibatis.session.SqlSession

import scala.collection.mutable.ArrayBuffer

class NodeCrudOperator extends CrudOperator with Logging {
  private val baseCrudOperator = new CrudOperatorTemplate()
  def getServerCluster(input: ErServerCluster, sqlSession: SqlSession = null): ErServerCluster = {
    baseCrudOperator.doCrudOperationSingleResult(NodeCrudOperator.doGetServerCluster, input, sqlSession)
  }

  def createServerNode(input: ErServerNode): ErServerNode = {
    baseCrudOperator.doCrudOperationSingleResult(NodeCrudOperator.doCreateServerNode, input, openTransaction = true)
  }

  def getServerNode(input: ErServerNode): ErServerNode = {
    val nodeResult = baseCrudOperator.doCrudOperationMultipleResult(NodeCrudOperator.doGetServerNodes, input)

    if (nodeResult.nonEmpty) {
      nodeResult(0)
    } else {
      null
    }
  }

  def getServerNodes(input: ErServerNode): Array[ErServerNode] = {
    baseCrudOperator.doCrudOperationMultipleResult(NodeCrudOperator.doGetServerNodes, input)
  }
}

object NodeCrudOperator {
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
}
