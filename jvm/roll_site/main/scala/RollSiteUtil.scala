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
package com.webank.eggroll.rollsite

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.protobuf.ByteString
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.datastructure.LinkedBlockingBroker
import com.webank.eggroll.core.meta.ErRollSiteHeader
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.rollpair.{RollPair, RollPairContext}
import com.webank.eggroll.rollsite.infra.JobStatus


class RollSiteUtil(val erSessionId: String,
                   rollSiteHeader: ErRollSiteHeader,
                   options: Map[String, String] = Map.empty) extends Logging {
  private val session = RollSiteUtil.sessionCache.get(erSessionId)
  private val ctx = new RollPairContext(session)
  //private val nameStripped = name
  val namespace = rollSiteHeader.rollSiteSessionId
  val name = rollSiteHeader.concat()

  logDebug("scalaPutBatch name: " + name + ", namespace: " + namespace)
  val rp: RollPair = ctx.load(namespace, name, options = rollSiteHeader.options)

  Runtime.getRuntime.addShutdownHook(new Thread(){
    override def run(): Unit = {
      // TODO:0: un comment
      //      session.stop
      //      ctx.stop
    }
  })

  def putBatch(value: ByteBuffer): Unit = {
    putBatch(ByteString.copyFrom(value))
  }

  def putBatch(value: ByteString): Unit = {
    try {
      val srcPartyId = rollSiteHeader.srcPartyId
      val dstPartyId = rollSiteHeader.dstPartyId
      if (!srcPartyId.equals(dstPartyId) || !rollSiteHeader.dataType.toLowerCase.equals("object")) {
        if (value.size() == 0) {
          throw new IllegalArgumentException(s"roll site push batch zero size: ${name}")
        }
        val broker = new LinkedBlockingBroker[ByteString]()
        broker.put(value)
        broker.signalWriteFinish()
        rp.putBatch(broker, options = options)
      } else {
        logInfo(s"sending OBJECT from / to same party id, skipping. src: ${srcPartyId}, dst: ${dstPartyId}, tag: ${rollSiteHeader.concat()}")
      }

      JobStatus.increasePutBatchFinishedCount(name);
      logInfo(s"put batch finished for name: ${name}, namespace: ${namespace}")
    } catch {
      case e: Exception => {
        logError(e)
        throw new RuntimeException(e)
      }
    }
  }

}

object RollSiteUtil {
  // todo: consider session closed
  val sessionCache: LoadingCache[String, ErSession] = CacheBuilder.newBuilder
    .maximumSize(1000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .concurrencyLevel(30)
    .recordStats
    .softValues
    .build(new CacheLoader[String, ErSession]() {
    override def load(key: String): ErSession = {
      new ErSession(sessionId = key, createIfNotExists = false)
    }
  })
}