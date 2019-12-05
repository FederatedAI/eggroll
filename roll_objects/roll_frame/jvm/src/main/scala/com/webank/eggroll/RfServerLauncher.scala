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

package com.webank.eggroll

import java.util.Random

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.io.adapter.HdfsBlockAdapter
import com.webank.eggroll.core.meta.{ErPartition, ErStore, ErStoreLocator}
import com.webank.eggroll.format.{FrameBatch, FrameDB, FrameSchema}
import com.webank.eggroll.rollframe.{ClusterManager, RollFrame, RollFrameClientMode}
import java.net.InetAddress

class RfServerLauncher {

}

object RfServerLauncher {
  private val clusterManager = {
    ClusterManager.setMode("cluster")
    val localhost: InetAddress = InetAddress.getLocalHost
    val localIpAddress: String = localhost.getHostAddress
    val hostname = localhost.getHostName
    println(s"Host IP: $localIpAddress")
    println(s"Host Name: $hostname")
    new ClusterManager
  }

  val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS)
  val input = ErStore(storeLocator = storeLocator,
    partitions = Array(
      ErPartition(id = 0, storeLocator = storeLocator, processor = clusterManager.clusterNode0),
      ErPartition(id = 1, storeLocator = storeLocator, processor = clusterManager.clusterNode1),
      ErPartition(id = 2, storeLocator = storeLocator, processor = clusterManager.clusterNode2)))

  /**
    * Test rollframe some function in the cluster with three processor.
    *
    * @param args :start server  : java -cp eggroll-rollframe.jar com.webank.eggroll.RfServerLauncher server 0 _
    *             run client job: java -cp eggroll-rollframe.jar com.webank.eggroll.RfServerLauncher client 0 c/v1/v2
    */
  def main(args: Array[String]): Unit = {
    val mode = args(0).toLowerCase() // server/client
    val nodeId = args(1).toLong // 0,1,2
    val taskType = args(2).toLowerCase() // map,reduce,aggregate

    // whether is't client mode
    mode match {
      case "client" => {
        println("Client Mode...")
        clientTask(taskType)
      }
      case _ => {
        println(s"Start RollFrame Servant, server id = $nodeId")
        clusterManager.startServerCluster(nodeId = nodeId)
      }
    }
  }

  def clientTask(name: String): Unit = {
    name match {
      case "c" => createHdfsData()
      case "v1" => verifyHdfsToJvm()
      case "v2" => verifyNetworkToJvm()
      case _ => throw new UnsupportedOperationException("not such task...")
    }
  }

  def verifyHdfsToJvm(): Unit = {
    var start = System.currentTimeMillis()
    val cacheStore = RollFrame.Util.loadCache(input)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))


    val outStoreLocator = ErStoreLocator(name = "v1", namespace = "test1", storeType = StringConstants.HDFS)
    val output = input.copy(storeLocator = outStoreLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = outStoreLocator)))

    println("begin run jvm to hdfs")
    start = System.currentTimeMillis()
    val rf1 = new RollFrameClientMode(cacheStore)
    rf1.mapBatch(cb => cb, output = output)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))
    // check hdfs whether has "v1" store
  }

  def verifyNetworkToJvm(): Unit = {
    val fbs = (0 until 3).map(i => FrameDB(input, i).readAll())

    val networkLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.NETWORK)
    val networkStore = input.copy(storeLocator = networkLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = networkLocator)))

    var start = System.currentTimeMillis()
    fbs.indices.foreach(i => FrameDB(networkStore, i).writeAll(fbs(i)))
    println("finish fbs to network, time: " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    val cacheStore = RollFrame.Util.loadCache(networkStore)
    println("finish network to jvm, time: " + (System.currentTimeMillis() - start))

    val outStoreLocator = ErStoreLocator(name = "v2", namespace = "test1", storeType = StringConstants.HDFS)
    val output = input.copy(storeLocator = outStoreLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = outStoreLocator)))

    start = System.currentTimeMillis()
    val rf1 = new RollFrameClientMode(cacheStore)
    rf1.mapBatch(cb => cb, output = output)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))
    // check hdfs whether has "v1" store
  }

  def createHdfsData(): Unit = {
    val fieldCount = 10
    val rowCount = 100 // total value count = rowCount * fbCount * fieldCount
    val fbCount = 1 // the num of batch

    def write(adapter: FrameDB): Unit = {
      val randomObj = new Random()
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(getSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
        }
        println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
        adapter.append(fb)
      }
      adapter.close()
    }

    def read(adapter: FrameDB): Unit = {
      var num = 0
      adapter.readAll().foreach(_ => num += 1)
      val oneFb = adapter.readOne()
      adapter.close()
      assert(fbCount == num)
      assert(fieldCount == oneFb.fieldCount)
      assert(rowCount == oneFb.rowCount)
    }

    val output = ErStore(storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS))

    write(FrameDB(output, 0))
    write(FrameDB(output, 1))
    write(FrameDB(output, 2))
    read(FrameDB(output, 0))
    read(FrameDB(output, 1))
    read(FrameDB(output, 2))
  }


  private def getSchema(fieldCount: Int): String = {

    val sb = new StringBuilder
    sb.append("""{"fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }
}
