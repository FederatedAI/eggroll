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

package com.webank.eggroll.core.clustermanager

import java.net.InetSocketAddress

import com.webank.eggroll.core.clustermanager.metadata.{ServerNodeCrudOperator, StoreCrudOperator}
import com.webank.eggroll.core.clustermanager.session.SessionManager
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{MetadataCommands, SessionCommands}
import com.webank.eggroll.core.meta.{ErProcessorBatch, ErServerCluster, ErServerNode, ErSessionMeta, ErStore}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcTransferService
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

object ClusterManager extends Logging {
  def registerRouter():Unit = {
    CommandRouter.register(serviceName = MetadataCommands.getServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getServerNode)

    CommandRouter.register(serviceName = MetadataCommands.getServerNodesServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerCluster]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getServerNodes)

    CommandRouter.register(serviceName = MetadataCommands.getOrCreateServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getOrCreateServerNode)

    CommandRouter.register(serviceName = MetadataCommands.createOrUpdateServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.createOrUpdateServerNode)

    CommandRouter.register(serviceName = MetadataCommands.getStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.getStore)

    CommandRouter.register(serviceName = MetadataCommands.getOrCreateStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.getOrCreateStore)

    CommandRouter.register(serviceName = MetadataCommands.deleteStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.deleteStore)

    CommandRouter.register(serviceName = SessionCommands.getOrCreateSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[ClusterManager],
      routeToMethodName = SessionCommands.getOrCreateSession.getName())

    CommandRouter.register(serviceName = SessionCommands.registerSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta], classOf[ErProcessorBatch]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[ClusterManager],
      routeToMethodName = SessionCommands.registerSession.getName())
  }
  // TODO: wrap server
  def buildServer(args: Array[String]): Server = {
    val cmd = MiscellaneousUtils.parseArgs(args = args)
    val portString = cmd.getOptionValue('p', "4670")
    registerRouter()
    val clusterManager = NettyServerBuilder
      .forAddress(new InetSocketAddress("127.0.0.1", portString.toInt))
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .maxInboundMessageSize(1024 * 1024 *1024)
      .maxInboundMetadataSize(1024 * 1024)
      .build()

    val server: Server = clusterManager.start()

    val port = clusterManager.getPort
    StaticErConf.setPort(port)

    val confPath = cmd.getOptionValue('c', "./jvm/core/main/resources/cluster-manager.properties")
    StaticErConf.addProperties(confPath)
    logInfo(s"server started at port ${port}")
    println(s"server started at port ${port}")
    server
  }
  def main(args: Array[String]): Unit = {
    buildServer(args).awaitTermination()
  }
}

class ClusterManager {
  def getOrCreateSession(sessionMeta: ErSessionMeta): ErProcessorBatch = {
    SessionManager.getOrCreateSession(sessionMeta)
  }

  def registerSession(sessionMeta: ErSessionMeta, processorBatch: ErProcessorBatch): ErProcessorBatch = {
    SessionManager.register(sessionMeta = sessionMeta, processorBatch = processorBatch)
  }
}