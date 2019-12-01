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

package com.webank.eggroll.rollpair.io

import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.{NetworkConstants, StoreTypes}
import com.webank.eggroll.core.datastructure.LinkedBlockingBroker
import com.webank.eggroll.core.meta.{ErStore, ErStoreLocator}
import com.webank.eggroll.rollpair.client.RollPair
import org.junit.Test

import scala.collection.mutable.ListBuffer

class TestIo {
  val partitionId = 1
  //val dbPath: String = "/tmp/eggroll/levelDb/ns/name/" + partitionId
  //val rootPath = s"/tmp/eggroll/levelDb/ns"
  val rootPath = s"/tmp/eggroll/${StoreTypes.ROLLPAIR_LEVELDB}/namespace"

  val namePath = s"${rootPath}/name/"
  val testPath = s"${rootPath}/test/"
  val mapValuesPath: String = s"${rootPath}/testMapValues/"
  val reducePath: String = s"${rootPath}/testReduce/"
  val joinPath: String = s"${rootPath}/testJoin/"
  val mapPath: String = s"${rootPath}/testMap/"
  val dbPath = mapValuesPath
  val rocksDBSortedKVAdapter: RocksdbSortedKvAdapter = new RocksdbSortedKvAdapter(dbPath)

  val hello = "hello"
  val world = "world"

  @Test
  def testRocksDbPut(): Unit = {
    // val rocksDBSortedKVAdapter: RocksDBSortedKVAdapter = new RocksDBSortedKVAdapter()
    val key: Array[Byte] = hello.getBytes
    val value: Array[Byte] = world.getBytes
    rocksDBSortedKVAdapter.put(key, value)
  }

  @Test
  def testRocksDbGet(): Unit = {
    // val rocksDBSortedKVAdapter: RocksDBSortedKVAdapter = new RocksDBSortedKVAdapter()
    val key = hello.getBytes()
    val value = rocksDBSortedKVAdapter.get(key)

    println(if (value == null) "null" else new String(value))
  }

  @Test
  def testWriteBatch(): Unit = {
    val batch = ListBuffer[(Array[Byte], Array[Byte])]()
    for (i <- 0 to 10) {
      batch.append(((s"k-${partitionId}-${i}").getBytes(), (s"v-${partitionId}-${i}").getBytes()))
    }

    rocksDBSortedKVAdapter.writeBatch(batch.iterator)
  }

  @Test
  def testIterate(): Unit = {
    val iter = rocksDBSortedKVAdapter.iterate()
    while (iter.hasNext) {
      val next = iter.next()
      println("key: " + new String(next._1) + ", value: " + new String(next._2))
    }
  }

  @Test
  def testPutBatch(): Unit = {
    val input = ErStore(ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "name"))
    val rp = new RollPair(input)

    var directBinPacketBuffer: ByteBuffer = ByteBuffer.allocateDirect(1<<10)
    directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_MAGIC_NUMBER)   // magic num
    directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_VERSION)     // protocol version
    directBinPacketBuffer.putInt(4)   // header length
    directBinPacketBuffer.putInt(16)  // body size
    directBinPacketBuffer.putInt(4)   // key length (bytes)
    directBinPacketBuffer.putInt(3)   // key
    directBinPacketBuffer.putInt(4)   // value length (bytes)
    directBinPacketBuffer.putInt(5)   // value

    directBinPacketBuffer.flip()

    val broker = new LinkedBlockingBroker[ByteString]()
    broker.put(ByteString.copyFrom(directBinPacketBuffer))
    rp.putBatch(broker)
  }

  @Test
  def testWriteMultipleKvBatch(): Unit = {
    val path = namePath
    for (p <- 0 until 4) {
      val partitionAdapter = new RocksdbSortedKvAdapter(path + p)
      val batch = ListBuffer[(Array[Byte], Array[Byte])]()
      for (i <- 0 to 10) {
        batch.append(((s"k-${p}-${i}").getBytes(), (s"v-${p}-${i}").getBytes()))
      }

      partitionAdapter.writeBatch(batch.iterator)
      partitionAdapter.close()
    }
  }

  @Test
  def testIterateMultipleKvBatch(): Unit = {
    val path = namePath
    println(s"path: ${path}")

    for (p <- 0 until 4) {
      val partitionAdapter = new RocksdbSortedKvAdapter(path + p)
      var count = 0
      println(s"partition #${p}:")

      val iter = partitionAdapter.iterate()
      while (iter.hasNext) {
        val next = iter.next()
        println(s"key: ${new String(next._1)}, value: ${new String(next._2)}")
        count += 1
      }

      println(s"total count: ${count}")
      println()
      partitionAdapter.close()
    }
  }
}
