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
  val dbPath: String = "/tmp/eggroll/ns/name/0"
  val rocksDBSortedKVAdapter: RocksDBSortedKvAdapter = new RocksDBSortedKvAdapter(dbPath)

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
      batch.append((("k" + i).getBytes(), ("v" + i).getBytes()))
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
}
