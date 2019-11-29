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

import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import com.google.common.base.Predicates
import com.webank.eggroll.core.constant.{ProcessorStatus, SessionConfKeys}
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErSessionMeta}
import com.webank.eggroll.core.retry.factory.{RetryerBuilder, StopStrategies, WaitTimeStrategies}
import com.webank.eggroll.core.schedule.deploy.PythonProcessorOperator
import com.webank.eggroll.core.session.RuntimeErConf

import scala.collection.mutable.ArrayBuffer


// todo: consider multiple roll object types. maybe add another layer of map
object ProcessorManager {
  private val initializedSession = new ConcurrentHashMap[String, ErProcessorBatch]()
  private val initializingSession = new ConcurrentHashMap[String, (util.Set[ErProcessor], CountDownLatch)]()
  private val initializingLock = new Object()

  def getOrCreateProcessorBatch(sessionMeta: ErSessionMeta): ErProcessorBatch = {
    val sessionId = sessionMeta.id

    if (initializedSession.containsKey(sessionId)) {
      return initializedSession.get(sessionId)
    }

    var iAmStarter = false
    if (!initializingSession.containsKey(sessionId)) {
      initializingLock.synchronized {
        if (!initializingSession.containsKey(sessionId)) {
          iAmStarter = true
          val runtimeConf = new RuntimeErConf(sessionMeta)
          // todo: consider different processor types

          val maxProcessorsPerNode = runtimeConf.getInt(SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE, 1)
          val finalProcessorCount = Math.min(Runtime.getRuntime.availableProcessors(), maxProcessorsPerNode)
          initializingSession.putIfAbsent(sessionId, (Collections.synchronizedSet(new util.HashSet[ErProcessor](finalProcessorCount)), new CountDownLatch(finalProcessorCount)))
          var totalStarted = 0
          (0 until finalProcessorCount).foreach(i => {
            val operator = new PythonProcessorOperator()
            if (operator.start(runtimeConf)) {
              totalStarted += 1
            }
          })

        }
      }
    }
    // todo: decide what to do if totalStarted < finalProcessorCount

    // todo: configurable
    val processStartupRetryer = RetryerBuilder.newBuilder()
      .withStopStrategy(StopStrategies.stopAfterMaxAttempt(5))
      .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(3, TimeUnit.SECONDS))
      .withDescription("waiting for processors to heartbeat")
      .retryIfResult(Predicates.equalTo(false))
      .retryIfAnyException()
      .build()

    processStartupRetryer.call(() => {
      val tuple = initializingSession.get(sessionId)

      tuple == null || tuple._2.await(3, TimeUnit.SECONDS)
    })

    initializedSession.get(sessionId)
  }

  def registerProcessor(processor: ErProcessor): Unit = {
    val sessionId = processor.options.get(SessionConfKeys.CONFKEY_SESSION_ID)
    if (!initializingSession.containsKey(sessionId)) {
      throw new IllegalStateException(s"sessionId ${sessionId} is not initializing")
    }

    if (!ProcessorStatus.RUNNING.equals(processor.status)) {
      throw new IllegalStateException(s"processor is not running. current status: ${processor.status}")
    }

    val (set: util.Set[ErProcessor], latch) = initializingSession.get(sessionId)

    set.add(processor)

    if (latch.getCount == 1) {
      val processorBatch = ErProcessorBatch(tag = sessionId, processors = set.toArray(Array[ErProcessor]()))
      initializedSession.put(sessionId, processorBatch)
      initializingSession.remove(sessionId)
    }

    latch.countDown()
  }

  def removeProcessorBatch(sessionId: String): ErProcessorBatch = {
    val processorBatch = initializedSession.remove(sessionId)
    if (processorBatch == null) {
      return null
    }
    val operator = new PythonProcessorOperator()
    val terminatedProcessors = ArrayBuffer[ErProcessor]()
    terminatedProcessors.sizeHint(processorBatch.processors.length)

    processorBatch.processors.foreach(p => {
      val stopResult = operator.stop(p)
      if (stopResult) terminatedProcessors += p.copy(status = ProcessorStatus.TERMINATED)
      else terminatedProcessors += p.copy(status = ProcessorStatus.ERROR)
    })

    processorBatch.copy(processors = terminatedProcessors.toArray)
  }

  def isSessionInitializing(sessionId: String): Boolean = {
    initializingSession.containsKey(sessionId)
  }
}
