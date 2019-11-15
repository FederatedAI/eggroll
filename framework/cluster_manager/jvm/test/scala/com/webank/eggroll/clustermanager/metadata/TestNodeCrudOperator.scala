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

import com.webank.eggroll.core.meta.{ErEndpoint, ErServerNode, ErServerCluster}
import com.webank.eggroll.core.session.DefaultErConf
import org.junit.Test

class TestNodeCrudOperator {
  println(new File(".").getAbsolutePath)
  DefaultErConf.addProperties("main/resources/cluster-manager.properties")
  val nodeCrudOperator = new NodeCrudOperator

  @Test
  def testGetServerCluster(): Unit = {
    val input = ErServerCluster(0)
    val result = nodeCrudOperator.getServerCluster(input)

    println(result)
    result.serverNodes.foreach(println)
  }

  @Test
  def testCreateNode(): Unit = {
    val input = ErServerNode(endpoint = ErEndpoint(host = "localhost", port = 8310))
    val result = nodeCrudOperator.createServerNode(input)

    println(result)
  }

  @Test
  def testGetNode(): Unit = {
    val input = ErServerNode(id = 1)
    val result = nodeCrudOperator.getServerNode(input)

    print(result)
  }

  @Test
  def testGetNodes(): Unit = {
    val input = ErServerNode(endpoint = ErEndpoint("localhost"))
    val result = nodeCrudOperator.getServerNodes(input)
    result.foreach(n => println(n))
  }
}
