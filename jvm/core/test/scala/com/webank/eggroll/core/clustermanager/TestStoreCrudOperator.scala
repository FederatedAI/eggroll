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

import com.webank.eggroll.core.constant.StoreTypes
import com.webank.eggroll.core.meta.{ErStore, ErStoreLocator}
import com.webank.eggroll.core.resourcemanager.metadata.StoreCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import org.junit.Test

class TestStoreCrudOperator {

  println(new File(".").getAbsolutePath)
  StaticErConf.addProperties("main/resources/cluster-manager.properties")
  val storeCrudOperator = new StoreCrudOperator
  @Test
  def testGetStore(): Unit = {
    val input = ErStore(ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "name"))
    val result = storeCrudOperator.getStore(input)

    println(result)
    result.partitions.foreach(println)
  }

  @Test
  def testInsertStore(): Unit = {
    val proposedStore = ErStore(ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "test", totalPartitions = 4))
    val result = storeCrudOperator.getOrCreateStore(proposedStore)

    print(result)
  }
}