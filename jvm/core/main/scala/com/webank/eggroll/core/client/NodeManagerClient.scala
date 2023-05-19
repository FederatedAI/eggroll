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

import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.{ContainerCommands, NodeManagerCommands, NodeManagerConfKeys, ResouceCommands}
import com.webank.eggroll.core.containers.meta._
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.session.StaticErConf


class NodeManagerClient(var nodeManagerEndpoint: ErEndpoint) {
  if (nodeManagerEndpoint == null || !nodeManagerEndpoint.isValid)
    throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + nodeManagerEndpoint)

  private val commandClient = new CommandClient(nodeManagerEndpoint)

  def this(serverHost: String, serverPort: Int) {
    this(new ErEndpoint(serverHost, serverPort))
  }

  def this() {
    this(StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST, "localhost"),
      StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1))
  }

  def heartbeat(processor: ErProcessor): ErProcessor =
    commandClient.call[ErProcessor](NodeManagerCommands.heartbeat, processor)

  def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta =
    commandClient.call[ErSessionMeta](NodeManagerCommands.startContainers, sessionMeta)


  def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta =
    commandClient.call[ErSessionMeta](NodeManagerCommands.stopContainers, sessionMeta)

  def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta =
    commandClient.call[ErSessionMeta](NodeManagerCommands.killContainers, sessionMeta)


  def startJobContainers(startContainersRequest: StartContainersRequest): StartContainersResponse = {
    commandClient.call(ContainerCommands.startJobContainers, startContainersRequest)
  }

  def stopJobContainers(stopContainersRequest: StopContainersRequest): StopContainersResponse = {
    commandClient.call(ContainerCommands.stopJobContainers, stopContainersRequest)
  }

  def killJobContainers(killContainersRequest: KillContainersRequest): KillContainersResponse =
    commandClient.call(ContainerCommands.killJobContainers, killContainersRequest)

  def allocateResource(srcAllocate: ErResourceAllocation): ErResourceAllocation =
    commandClient.call[ErResourceAllocation](ResouceCommands.resourceAllocation, srcAllocate)

  def queryNodeResource(erServerNode: ErServerNode): ErServerNode =
    commandClient.call[ErServerNode](ResouceCommands.queryNodeResource, erServerNode)

  def downloadContainers(downloadContainersRequest: DownloadContainersRequest): DownloadContainersResponse =
    commandClient.call(ContainerCommands.downloadContainers, downloadContainersRequest)

  def checkNodeProcess(processor: ErProcessor): ErProcessor =
    commandClient.call[ErProcessor](ResouceCommands.checkNodeProcess, processor)
}
