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

package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.session.RuntimeErConf
import org.junit.{Before, Test}

class TestContainer {
  @Before
  def init(): Unit = {
    TestAssets.initConf()
  }

  @Test
  def testStartEggPair(): Unit = {
    val runtimeErConf = RuntimeErConf()
    val container = new Container(runtimeErConf, "egg_pair")

    val result = container.start()

    print(result)
  }

  @Test
  def testStartRollPairMaster(): Unit = {
    val runtimeErConf = RuntimeErConf()
    val container = new Container(runtimeErConf, "roll_pair_master")

    val result = container.start()

    print(result)
  }

  @Test
  def testStop(): Unit = {
    val runtimeErConf = RuntimeErConf()
    val container = new Container(runtimeErConf, "egg_pair", 2)

    val result = container.stop()
    print(result)
  }
}
