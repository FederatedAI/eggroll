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
  val name = rollSiteHeader.getRsKey()

  val rp: RollPair = ctx.load(namespace, name, options = rollSiteHeader.options)

  def putBatch(value: ByteBuffer): Unit = {
    putBatch(ByteString.copyFrom(value))
  }

  def putBatch(value: ByteString): Unit = {
    try {
      logDebug(s"put batch started for namespace=${namespace}, name=${name}")
      val srcPartyId = rollSiteHeader.srcPartyId
      val dstPartyId = rollSiteHeader.dstPartyId
      if (!srcPartyId.equals(dstPartyId) || !rollSiteHeader.dataType.toLowerCase.equals("object")) {
        if (value.size() == 0) {
          throw new IllegalArgumentException(s"roll site push batch zero size. namespace=${namespace}, name=${name}")
        }
        val broker = new LinkedBlockingBroker[ByteString]()
        broker.put(value)
        broker.signalWriteFinish()
       // rp.putBatch(broker, options = options)
      } else {
        logTrace(s"sending OBJECT from / to same party id, skipping. src=${srcPartyId}, dst=${dstPartyId}, tag=${rollSiteHeader.getRsKey()}")
      }

      val partition = rollSiteHeader.options.get("partition_id")

      partition match {
        case Some(s) =>
          val partitionId = partition.get.toInt
          JobStatus.increasePutBatchFinishedCountPerPartition(name, partitionId)
          logDebug(s"put batch finished for namespace=${namespace}, name=${name}, partitionId=${partitionId}")
        case None =>
          JobStatus.increasePutBatchFinishedCount(name)
          logDebug(s"put batch finished for namespace=${namespace}, name=${name}, partitionId=UNKNOWN")
      }
    } catch {
      case e: Exception => {
        JobStatus.addJobError(name, e)
        logError(s"put batch error for namespace=${namespace}, name=${name}", e)
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