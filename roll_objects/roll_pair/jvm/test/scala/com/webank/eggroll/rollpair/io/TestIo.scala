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
 */

package com.webank.eggroll.rollpair.io

import org.junit.Test

import scala.collection.mutable.ListBuffer

class TestIo {
  val partitionId = 0
  //val dbPath: String = "/tmp/eggroll/levelDb/ns/name/" + partitionId

  val dbPathPrefix = "/tmp/eggroll/levelDb/ns/test/"
  val mapValuesPath: String = "/tmp/eggroll/levelDb/ns/testMapValues/"
  val reducePath: String = "/tmp/eggroll/levelDb/ns/testReduce/"
  val joinPath: String = "/tmp/eggroll/levelDb/ns/testJoin/"
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
  def testWriteMultipleKvBatch(): Unit = {
    for (p <- 0 until 4) {
      val partitionAdapter = new RocksdbSortedKvAdapter(dbPathPrefix + p)
      val batch = ListBuffer[(Array[Byte], Array[Byte])]()
      for (i <- 0 to 10) {
        batch.append(((s"k-${partitionId}-${i}").getBytes(), (s"v-${partitionId}-${i}").getBytes()))
      }

      partitionAdapter.writeBatch(batch.iterator)
      partitionAdapter.close()
    }
  }

  @Test
  def testIterateMultipleKvBatch(): Unit = {
    val path = joinPath
    println(s"path: ${path}")
    for (p <- 0 until 4) {
      val partitionAdapter = new RocksdbSortedKvAdapter(path + p)
      println(s"partition #${p}:")

      val iter = partitionAdapter.iterate()
      while (iter.hasNext) {
        val next = iter.next()
        println(s"key: ${new String(next._1)}, value: ${new String(next._2)}")
      }

      println()
      partitionAdapter.close()
    }
  }
}
