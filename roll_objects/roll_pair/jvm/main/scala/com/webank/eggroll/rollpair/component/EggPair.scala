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

package com.webank.eggroll.rollpair.component

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErPartition, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaFunctorSerdes
import com.webank.eggroll.rollpair.io.RocksDBSortedKvAdapter

import scala.collection.mutable

class EggPair {
  def runTask(task: ErTask): Unit = {
    val functors = task.job.functors

    val results = mutable.ListBuffer()

    if (task.id == "mapValues") {
      val f: Array[Byte] => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)
      val inputPartition = task.inputs.head
      val outputPartition = task.outputs.head

      val inputStore = new RocksDBSortedKvAdapter(getDbPath(inputPartition))
      val outputStore = new RocksDBSortedKvAdapter(getDbPath(outputPartition))

      outputStore.writeBatch(inputStore.iterate().map(t => (t._1, f(t._2))))

      inputStore.close()
      outputStore.close()
    }
  }

  def getDbPath(partition: ErPartition): String = {
    val storeLocator = partition.storeLocator
    val dbPathPrefix = "/tmp/eggroll/"
    dbPathPrefix + String.join(StringConstants.SLASH, storeLocator.storeType, storeLocator.namespace, storeLocator.name, partition.id)
  }
}

object EggPair {
  val functorSerdes = DefaultScalaFunctorSerdes()
}
