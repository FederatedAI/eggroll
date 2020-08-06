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

package com.webank.eggroll.rollsite.test

import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import com.webank.eggroll.rollsite.DataTransferClient
import org.junit.Test

import scala.collection.mutable.ListBuffer

class TestRollSiteClient extends Logging {
  private val headerBuilder = Proxy.Metadata.newBuilder()
  private val bodyBuilder = Proxy.Data.newBuilder()
  private val packetBuilder = Proxy.Packet.newBuilder()
  private val topicBuilder = Proxy.Topic.newBuilder()
  private val entryEndpoint = new ErEndpoint("localhost", 9370)
  private val client = new DataTransferClient(entryEndpoint)
  private val dstTopic = topicBuilder.setPartyId("10002").build()

  @Test
  def testUnaryCall(): Unit = {

    val result = client.unaryCall(packetBuilder
      .setHeader(headerBuilder.setSeq(12345).setAck(54321).setDst(dstTopic)).build())

    logInfo(s"unary call response: ${ToStringUtils.toOneLineString(result)}")
  }

  @Test
  def testPush(): Unit = {
    val data = ListBuffer[Proxy.Packet]()

    val seq = 10000
    val ack = 20000

    for (i <- 0 until 2) {
      data.append(packetBuilder.setHeader(headerBuilder.setSeq(seq + i).setAck(ack + i).setDst(dstTopic)).build())
    }

    val result = client.push(data.iterator)
    logInfo(s"push response: ${ToStringUtils.toOneLineString(result)}")
  }
}
