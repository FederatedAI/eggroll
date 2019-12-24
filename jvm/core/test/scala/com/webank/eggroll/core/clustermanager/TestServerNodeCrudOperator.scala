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

import com.webank.eggroll.core.meta.{ErEndpoint, ErServerCluster, ErServerNode}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import org.junit.Test

class TestServerNodeCrudOperator {
  println(new File(".").getAbsolutePath)
  StaticErConf.addProperties("main/resources/cluster-manager.properties")
  val nodeCrudOperator = new ServerNodeCrudOperator

  @Test
  def testGetServerCluster(): Unit = {
    val input = ErServerCluster(0)
    val result = nodeCrudOperator.getServerCluster(input)

    println(result)
    result.serverNodes.foreach(println)
  }

  @Test
  def testGetServerNode(): Unit = {
    val input = ErServerNode(id = 1)
    val result = nodeCrudOperator.getServerNode(input)

    println(result)
  }

  @Test
  def testGetServerNodes(): Unit = {
    val input = ErServerNode(endpoint = ErEndpoint("localhost"))
    val result = nodeCrudOperator.getServerNodes(input)
    println(result)
    println(result.serverNodes.foreach(println))
  }

  @Test
  def testGetOrCreateExistingServerNode(): Unit = {
    val input = ErServerNode(id = 1)
    val result = nodeCrudOperator.getServerNode(input)

    print(result)
  }

  @Test
  def testGetOrCreateNewServerNode(): Unit = {
    val input = ErServerNode(endpoint = ErEndpoint(host = "localhost", port = 4670))
    val result = nodeCrudOperator.getOrCreateServerNode(input)

    println(result)
  }
}