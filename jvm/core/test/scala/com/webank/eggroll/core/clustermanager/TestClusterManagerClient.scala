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

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap
import com.webank.eggroll.core.resourcemanager.metadata.{ServerNodeCrudOperator, StoreCrudOperator}
import com.webank.eggroll.core.session.StaticErConf
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.junit.{Assert, Before, Test}


class TestClusterManager {
  val clusterManagerHost = "localhost"
  val clusterManagerPort = 4670
  val clusterManagerClient = new ClusterManagerClient(clusterManagerHost, clusterManagerPort)

  @Before
  def setup(): Unit = {
    println(new File(".").getAbsolutePath)
    StaticErConf.addProperties("main/resources/cluster-manager.properties.local")
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
      routeToClass = classOf[ClusterManagerBootstrap],
      routeToMethodName = SessionCommands.getOrCreateSession.getName())

    CommandRouter.register(serviceName = SessionCommands.getSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[ClusterManagerBootstrap],
      routeToMethodName = SessionCommands.getSession.getName())

    CommandRouter.register(serviceName = SessionCommands.registerSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta], classOf[ErProcessorBatch]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[ClusterManagerBootstrap],
      routeToMethodName = SessionCommands.registerSession.getName())

    val clusterManager = NettyServerBuilder
      .forPort(clusterManagerPort)
      .addService(new CommandService)
      .maxInboundMetadataSize(1 << 20)
      .maxInboundMessageSize(-1)
      .build()

    StaticErConf.setPort(clusterManagerPort)
    val server: Server = clusterManager.start()
  }

  @Test
  def startAsService(): Unit = {
    println("meta service started ...")
    Thread.sleep(10000000)
  }

  @Test
  def testGetNonExistServerNode(): Unit = {
    val node = clusterManagerClient.getServerNode(ErServerNode(id = 9999999))

    print(node)
    Assert.assertNull(node)
  }

  @Test
  def testGetExistingServerNode(): Unit = {
    val node = clusterManagerClient.getServerNode(ErServerNode(id = 1))

    print(node)
    Assert.assertNotNull(node)
    Assert.assertEquals(node.id, 1)
  }

  @Test
  def testGetServerNodes(): Unit = {
    val nodes = clusterManagerClient.getServerNodes(ErServerNode(endpoint = ErEndpoint("localhost")))

    print(nodes)
    println(nodes)
    println(nodes.serverNodes.foreach(println))
  }

  @Test
  def testGetOrCreateServerNode(): Unit = {
    val input = ErServerNode(endpoint = ErEndpoint(host = "localhost", port = 9394), nodeType = ServerNodeTypes.NODE_MANAGER, status = ServerNodeStatus.HEALTHY)
    val node = clusterManagerClient.getOrCreateServerNode(input)

    println(node)
  }

  @Test
  def testCreateOrUpdateServerNode(): Unit = {
    val input = ErServerNode(id = 2, endpoint = ErEndpoint(host = "localhost", port = 9394), nodeType = ServerNodeTypes.NODE_MANAGER, status = ServerNodeStatus.HEALTHY)
    val node = clusterManagerClient.createOrUpdateServerNode(input)

    println(node)
  }

  @Test
  def testGetStore(): Unit = {
    val options = new ConcurrentHashMap[String, String]()
    val input = ErStore(storeLocator = ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LMDB, namespace = "namespace", name = "name"))

    val result = clusterManagerClient.getStore(input)

    println(result)
    result.partitions.foreach(println)

    val inputAgain = input.copy(options = result.options)
    val resultAgain = clusterManagerClient.getStore(inputAgain)
    println(resultAgain)
    resultAgain.partitions.foreach(println)
  }

  @Test
  def testGetOrCreateStore(): Unit = {
    println(System.getProperty("os.name"))
    val input = ErStoreLocator(
      storeType = StoreTypes.ROLLPAIR_LMDB,
      namespace = "namespace",
      name = System.currentTimeMillis().toString,
      totalPartitions = 4,
      partitioner = PartitionerTypes.BYTESTRING_HASH,
      serdes = SerdesTypes.PICKLE)

    val result = clusterManagerClient.getOrCreateStore(input)

    println(result)
    result.partitions.foreach(println)
  }

  @Test
  def testGetAndCreateWithSameBinding(): Unit = {
    val options = new ConcurrentHashMap[String, String]()
    val input = ErStore(storeLocator = ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "name"))

    val result = clusterManagerClient.getStore(input)

    println(result)
    result.partitions.foreach(println)

    val inputAgain = input.copy(storeLocator = input.storeLocator.copy(name = System.currentTimeMillis().toString),
      options = result.options)

    val resultAgain = clusterManagerClient.getOrCreateStore(inputAgain)

    println(resultAgain)
    resultAgain.partitions.foreach(println)
  }

  @Test
  def testDeleteStore(): Unit = {
    val input = ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "test")
    val result = clusterManagerClient.deleteStore(input)

    println(result)
  }
}