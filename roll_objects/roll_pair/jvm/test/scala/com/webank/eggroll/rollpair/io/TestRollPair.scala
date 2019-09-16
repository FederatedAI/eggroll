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

import com.webank.eggroll.core.meta.{ErPartition, ErServerNode, ErStore, ErStoreLocator}
import com.webank.eggroll.rollpair.component.RollPair
import org.junit.Test

import scala.collection.mutable

class TestRollPair {
  @Test
  def testMapValues(): Unit = {
    def append(value: String): String = {
      value + "1"
    }

    def appendByte(value: Array[Byte]): Array[Byte] = {
      val result: mutable.Buffer[Byte] = value.toBuffer
      result.append("1".getBytes().head)
      result.toArray
    }

    val storeLocator = ErStoreLocator("levelDb", "ns", "name")
    val partition = ErPartition(1.toString, storeLocator, ErServerNode())

    val rollPair = new RollPair(ErStore(storeLocator))

    val result = rollPair.mapValues(appendByte)

  }

}
