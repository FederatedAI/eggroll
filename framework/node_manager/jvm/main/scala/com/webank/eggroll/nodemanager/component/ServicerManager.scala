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

package com.webank.eggroll.nodemanager.component

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErSessionMeta}
import com.webank.eggroll.core.schedule.deploy.JvmProcessorOperator
import com.webank.eggroll.core.session.RuntimeErConf

object ServicerManager {
  private val initializedServicer = new ConcurrentHashMap[String, ErProcessorBatch]()
  private val initializingServicer = new ConcurrentHashMap[String, CountDownLatch]()

  def getOrCreateServicer(sessionMeta: ErSessionMeta): ErProcessorBatch = {
    val sessionId = sessionMeta.id
    val jvmProcessorOperator = new JvmProcessorOperator
    val runtimeConf = new RuntimeErConf(sessionMeta)

    initializingServicer.put(sessionId, new CountDownLatch(1))

    val result = jvmProcessorOperator.start(runtimeConf)

    if (!result) {
      throw new IllegalStateException("Error creating servicer")
    }

    println(s"ready to wait. sessionId: ${sessionId}")
    initializingServicer.get(sessionId).await(10, TimeUnit.SECONDS)

    println(s"got servicer. result: ${initializedServicer.get(sessionId)}")
    initializedServicer.get(sessionId)
  }

  def registerServicer(processor: ErProcessor): Unit = {
    val sessionId = processor.options.get(SessionConfKeys.CONFKEY_SESSION_ID)
    println(s"registering for sessionId: ${sessionId}")
    // todo: error check
    val latch = initializingServicer.remove(sessionId)

    if (latch.getCount == 1) {
      initializedServicer.put(sessionId, ErProcessorBatch(tag = sessionId, processors = Array(processor)))
      latch.countDown()
    }
  }

  def isServicerInitializing(sessionId: String): Boolean = {
    initializingServicer.containsKey(sessionId) && initializingServicer.get(sessionId).getCount > 0
  }
}
