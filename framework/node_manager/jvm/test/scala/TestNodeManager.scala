import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{DeployConfKeys, MetadataCommands, NodeManagerCommands, SessionConfKeys}
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.transfer.GrpcTransferService
import com.webank.eggroll.nodemanager.component.{NodeManagerServicer, ProcessorManager, ServicerManager}
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.junit.{Before, Test}

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

class TestNodeManager {
  private var nodeManagerClient: NodeManagerClient = _
  private var sessionMeta: ErSessionMeta = _
  @Before
  def setup(): Unit = {
    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateProcessorBatchServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.getOrCreateProcessorBatch)

    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateServicerServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.getOrCreateServicer)

    CommandRouter.register(serviceName = NodeManagerCommands.heartbeatServiceName,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.heartbeat)

    val clusterManager = NettyServerBuilder
      .forPort(9394)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()

    val server: Server = clusterManager.start()
    nodeManagerClient = new NodeManagerClient()

    val conf = new RuntimeErConf()
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH, "/Users/max-webank/env/venv")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH, "/tmp/eggroll")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH, "/Users/max-webank/git/eggroll/roll_pair/egg_pair.py")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH, "/Users/max-webank/git")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS, "com.webank.eggroll.rollpair.component.Main")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH, "/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/lib/*:/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/eggroll-roll-pair-2.0.jar:/Users/max-webank/git/eggroll-2.x/framework/node_manager/jvm/test/resources")
    conf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, "test")
    conf.addProperty(SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE, "100")

    sessionMeta = ErSessionMeta(id = "testing", options = conf.getAllAsMap)
  }

  @Test
  def startAsService(): Unit = {
    println("node manager started")
    Thread.sleep(1000000)
  }

  @Test
  def testGetOrCreateServicer(): Unit = {
    println("hello")

    val result = ServicerManager.getOrCreateServicer(sessionMeta)
    println(result)
  }

  @Test
  def testGetOrCreateServicerWithClient(): Unit = {
    val result = nodeManagerClient.getOrCreateServicer(sessionMeta)

    print(result)
  }

  @Test
  def testGetOrCreateProcessorBatch(): Unit = {
    val result = ProcessorManager.getOrCreateProcessorBatch(sessionMeta)
    print(result)
  }

  @Test
  def testGetOrCreateProcessorBatchWithClient(): Unit = {
    val result = nodeManagerClient.getOrCreateProcessorBatch(sessionMeta)
    print(result)
  }
}

