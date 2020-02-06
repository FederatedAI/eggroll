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

package com.webank.eggroll.core.client

import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, MetadataCommands, SessionCommands}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.session.StaticErConf

import scala.collection.JavaConverters._

class ClusterManagerClient(val endpoint: ErEndpoint) {
  if (endpoint == null || !endpoint.isValid) {
    throw new IllegalArgumentException(s"failed to create ClusterManagerClient for endpoint: ${endpoint}")
  }

  private val cc = new CommandClient(endpoint)
  private val EMPTY_PARTITION_ARRAY = Array[ErPartition]();

  def this(serverHost: String, serverPort: Int) {
    this(ErEndpoint(serverHost, serverPort));
  }

  // TODO:2: priority property getter
  def this(options: Map[String, String]) {
    this(options.getOrElse(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST,
      StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST,
        "")),
      options.getOrElse(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT,
        StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT,
          "-1")).toInt)
  }

  def this() {
    this(StaticErConf.getString(
      ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, ""),
      StaticErConf.getInt(
        ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, -1))
  }

  def getServerNode(input:ErServerNode):ErServerNode = cc.call[ErServerNode](MetadataCommands.GET_SERVER_NODE, input)

  def getServerNodes(input:ErServerNode):ErServerCluster = cc.call[ErServerCluster](MetadataCommands.GET_SERVER_NODES, input)

  def getOrCreateServerNode(input:ErServerNode):ErServerNode =
    cc.call[ErServerNode](MetadataCommands.GET_OR_CREATE_SERVER_NODE, input)

  def createOrUpdateServerNode(input: ErServerNode):ErServerNode =
    cc.call[ErServerNode](MetadataCommands.CREATE_OR_UPDATE_SERVER_NODE, input)

  def getStore(input:ErStoreLocator):ErStore =
    getStore(ErStore(input, EMPTY_PARTITION_ARRAY, Map[String, String]().asJava))

  def getStore(input: ErStore): ErStore = cc.call[ErStore](MetadataCommands.GET_STORE, input)

  def getOrCreateStore(input: ErStoreLocator): ErStore =
    getOrCreateStore(new ErStore(input, EMPTY_PARTITION_ARRAY, new ConcurrentHashMap[String, String]))

  def getOrCreateStore(input: ErStore): ErStore = cc.call[ErStore](MetadataCommands.GET_OR_CREATE_STORE, input)

  def deleteStore(input: ErStore): ErStore = cc.call[ErStore](MetadataCommands.DELETE_STORE, input)

  def deleteStore(input: ErStoreLocator): ErStore =
    deleteStore(new ErStore(input, EMPTY_PARTITION_ARRAY, new ConcurrentHashMap[String, String]))

  def getStoreFromNamespace(input: ErStore): ErStoreList = cc.call[ErStoreList](MetadataCommands.GET_STORE_FROM_NAMESPACE, input)

  def getStoreFromNamespace(input: ErStoreLocator): ErStoreList = {
    getStoreFromNamespace(new ErStore(input, EMPTY_PARTITION_ARRAY, new ConcurrentHashMap[String, String]))
  }
  def getOrCreateSession(sessionMeta: ErSessionMeta): ErSessionMeta =
    cc.call[ErSessionMeta](SessionCommands.getOrCreateSession, sessionMeta)

  def getSession(sessionMeta: ErSessionMeta): ErSessionMeta =
    cc.call[ErSessionMeta](SessionCommands.getSession, sessionMeta)

  def stopSession(sessionMeta: ErSessionMeta): ErSessionMeta =
    cc.call[ErSessionMeta](SessionCommands.stopSession, sessionMeta)

  def killSession(sessionMeta: ErSessionMeta): ErSessionMeta =
    cc.call[ErSessionMeta](SessionCommands.killSession, sessionMeta)

  def killAllSessions(): Unit =
    cc.call[ErSessionMeta](SessionCommands.killAllSessions, ErSessionMeta())

  def registerSession(sessionMeta: ErSessionMeta): ErSessionMeta =
    cc.call[ErSessionMeta](SessionCommands.registerSession, sessionMeta)

  // todo:0: heartbeat with process pid
  def heartbeat(processor: ErProcessor): ErProcessor =
    cc.call[ErProcessor](SessionCommands.heartbeat, processor)
}
