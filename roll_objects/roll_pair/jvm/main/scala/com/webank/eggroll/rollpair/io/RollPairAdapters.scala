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

import java.io.File

import com.webank.eggroll.core.io.adapter.{SortedKvAdapter, SortedKvIterator}
import org.rocksdb.{Options, RocksDB, WriteBatch, WriteOptions};

class RocksdbSortedKvAdapter(path: String) extends SortedKvAdapter {

  // val path = "/tmp/eggroll/testIo"
  val options = new Options().setCreateIfMissing(true)
  val file = new File(path)
  var db: RocksDB = _

  locally {
    RocksDB.loadLibrary();
    file.mkdirs()
    db = RocksDB.open(options, path)
  }

  override def get(key: Array[Byte]): Array[Byte] = {
    db.get(key)
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    db.put(key, value)
  }

  override def iterate(): RocksdbIterator = {
    new RocksdbIterator(this)
  }

  override def writeBatch(iter: Iterator[(Array[Byte], Array[Byte])]): Unit = {
    val writeOptions = new WriteOptions()
    val writeBatch = new WriteBatch()

    for ((k, v) <- iter) {
      writeBatch.put(k, v)
    }

    db.write(writeOptions, writeBatch)
    writeBatch.close()
  }

  override def close(): Unit = {
    if (db != null) {
      db.close()
      db = null
    }
  }
}

class RocksdbIterator(adapter: RocksdbSortedKvAdapter) extends SortedKvIterator {
  val iter = adapter.db.newIterator()
  iter.seekToFirst()

  override def hasNext: Boolean = {
    iter.isValid
  }

  override def next(): (Array[Byte], Array[Byte]) = {
    val result = (iter.key(), iter.value())
    iter.next()

    result
  }

  override def close(): Unit = {
    iter.close()
  }
}