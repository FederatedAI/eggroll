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

package com.webank.eggroll.rollframe

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErStore
import com.webank.eggroll.format.FrameDB

object TestAssets {
  val clusterManager = new ClusterManager

  def getDoubleSchema(fieldCount: Int): String = {
    val sb = new StringBuilder
    sb.append(
      """{
                 "fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }

  /**
    * mock -- use to load Cache on local mode
    * @param inStore  ErStore
    * @return
    */
  def loadCache(inStore: ErStore): ErStore = {
    val cacheStoreLocator = inStore.storeLocator.copy(storeType = StringConstants.CACHE)
    val cacheStore = inStore.copy(storeLocator = cacheStoreLocator, partitions = inStore.partitions.map(p =>
      p.copy(storeLocator = cacheStoreLocator)))
    inStore.partitions.indices.foreach { i =>
      val inputDB = FrameDB(inStore, i)
      val outputDB = FrameDB(cacheStore, i)
      outputDB.writeAll(inputDB.readAll())
      inputDB.close()
      outputDB.close()
    }
    cacheStore
  }
}
