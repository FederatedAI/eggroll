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

import java.util.concurrent.CountDownLatch
import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.Proxy.{Model, Task}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderToPbMessage
import com.webank.eggroll.core.meta.{ErEndpoint, ErRollSiteHeader}
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import com.webank.eggroll.rollsite.Util
import io.grpc.stub.StreamObserver
import org.json.JSONObject
import org.junit.Test

import java.util.Date
import scala.io.Source

class TestRollSiteClient extends Logging {
  private val headerBuilder = Proxy.Metadata.newBuilder()
  private val bodyBuilder = Proxy.Data.newBuilder()
  private val packetBuilder = Proxy.Packet.newBuilder()
  private val topicBuilder = Proxy.Topic.newBuilder()
  private val taskBuilder = Proxy.Task.newBuilder()
  private val modelBuilder = Proxy.Model.newBuilder()
  private val endpoint10001 = new ErEndpoint("localhost", 9370)
  private val topic10001 = topicBuilder.setPartyId("10001").build()

  private val endpoint10002 = new ErEndpoint("localhost", 9470)
  private val topic10002 = topicBuilder.setPartyId("10002").build()

  @Test
  def testGetRouteTable(): Unit = {
    val channel = GrpcClientUtils.getChannel(endpoint10001)
    val stub = DataTransferServiceGrpc.newBlockingStub(channel)

    val salt = BigInt(new Date().getTime).toByteArray
    val key = "eggroll"
    val md5HashKey = Util.hashMD5(ByteString.copyFrom(salt).toStringUtf8 + key)

    val data = Proxy.Data.newBuilder().setKey(md5HashKey).setValue(ByteString.copyFrom(salt))
    val packet = packetBuilder.setHeader(Proxy.Metadata.newBuilder()
      .setDst(Proxy.Topic.newBuilder().setPartyId("10002")).setOperator("get_route_table")).setBody(data).build()

    val ret_packet = stub.unaryCall(packet)
    println(s"result=${ret_packet.getBody.getValue.toStringUtf8}")
  }

  @Test
  def testSetRouteTable(): Unit = {
    val source = Source.fromFile("/Users/bryce/projects/eggroll_guest/conf/route_table_test.json")
    val str = source.mkString
    val js = new JSONObject(str)

    val channel = GrpcClientUtils.getChannel(endpoint10001)
    val stub = DataTransferServiceGrpc.newBlockingStub(channel)

    val salt = BigInt(new Date().getTime).toByteArray
    val key = "eggroll"
    val md5HashKey = Util.hashMD5(ByteString.copyFrom(salt).toStringUtf8 + str + key)
    val data = Proxy.Data.newBuilder().setKey(md5HashKey).setValue(ByteString.copyFromUtf8(ByteString.copyFrom(salt).toStringUtf8 + str))

    val packet = packetBuilder.setHeader(Proxy.Metadata.newBuilder().setDst(Proxy.Topic.newBuilder().setPartyId("10002")).setOperator("set_route_table"))
      .setBody(data).build()

    val ret_packet = stub.unaryCall(packet)
    println(s"${ret_packet}")
    source.close()
  }

  @Test
  def testUnaryCall(): Unit = {
    val channel = GrpcClientUtils.getChannel(endpoint10001)
    val stub = DataTransferServiceGrpc.newBlockingStub(channel)

    val result = stub.unaryCall(packetBuilder
      .setHeader(headerBuilder.setSeq(12345).setAck(54321).setDst(topic10001)).build())

    logInfo(s"unary call response: ${ToStringUtils.toOneLineString(result)}")
  }

  @Test
  def testPush(): Unit = {
    val channel = GrpcClientUtils.getChannel(endpoint10001)
    val stub = DataTransferServiceGrpc.newStub(channel)
    val finishLatch = new CountDownLatch(1)

    val streamObserver = stub.push(new StreamObserver[Proxy.Metadata] {

      override def onNext(v: Proxy.Metadata): Unit = {

        logInfo(s"response: ${ToStringUtils.toOneLineString(v)}")
      }

      override def onError(throwable: Throwable): Unit = {
        finishLatch.countDown()
        logError(throwable)
      }

      override def onCompleted(): Unit = {
        finishLatch.countDown()
        logInfo("complete")
      }
    })

    val seq = 0

    for (i <- 0 until 10) {
      val rollsiteHeader = ErRollSiteHeader(
        rollSiteSessionId = "testing-guest",
        name = "transfer.variable",
        tag = "fit.1.0",
        srcRole = "guest",
        srcPartyId = "10000",
        dstRole = "host",
        dstPartyId = "10001",
        dataType = "rollpair",
        options = Map.empty,
        totalPartitions = 1,
        partitionId = 0,
        totalStreams = 3,
        totalBatches = 3,
        streamSeq = 0,
        batchSeq = 0,
        stage = "")
      streamObserver.onNext(
        packetBuilder.setHeader(
          headerBuilder
            .setSeq(seq + i)
            .setDst(topic10002)
            .setExt(rollsiteHeader.toProto().toByteString).build())
          .setBody(Proxy.Data.getDefaultInstance)
          .build())
    }
    logInfo("client complete")
//    Thread.sleep(3000)
    streamObserver.onCompleted()
    finishLatch.await()

    logInfo(s"finish test push")
  }

  @Test
  def testInitJobIdToSession(): Unit = {
    val request = packetBuilder
      .setHeader(headerBuilder.setSeq(12345).setDst(topic10001).setOperator("init_job_session_pair").setTask(Task.newBuilder().setModel(Model.newBuilder().setName("test_job").setDataKey("testing")))).build()

/*    val response = client10001.unaryCall(request)
    logInfo(s"init jobid to session result: ${ToStringUtils.toOneLineString(response)}")*/
  }
}
