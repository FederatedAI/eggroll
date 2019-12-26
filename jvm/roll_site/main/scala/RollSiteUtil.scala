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

import com.google.protobuf.ByteString
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.datastructure.LinkedBlockingBroker
import com.webank.eggroll.core.meta.ErSessionMeta
import com.webank.eggroll.rollpair.{RollPair, RollPairContext}


class RollSiteUtil(val session_id: String, name:String, namespace:String) {
  //private val clusterManagerClient = new ClusterManagerClient()
  //private val session =  clusterManagerClient.getSession(session_meta)
  private val session =  new ErSession(sessionId = session_id, createIfNotExists = false)
  private val ctx = new RollPairContext(session)
  private val nameStripped = name.substring("roll_site__".length)
  println("scalaPutBatch  name:" + nameStripped + ",namespace:" + namespace)
  val rp:RollPair = ctx.load(namespace, nameStripped)

  Runtime.getRuntime.addShutdownHook(new Thread(){
    override def run(): Unit = {
      // TODO:0: un comment
      //      session.stop
      //      ctx.stop
    }
  })

  def putBatch(value:ByteBuffer): Unit = {
    val directBinPacketBuffer: ByteBuffer = ByteBuffer.allocateDirect(1<<10)
    directBinPacketBuffer.put(value)

    directBinPacketBuffer.flip()

    val broker = new LinkedBlockingBroker[ByteString]()
    broker.put(ByteString.copyFrom(directBinPacketBuffer))
    broker.signalWriteFinish()
    rp.putBatch(broker)
  }
}