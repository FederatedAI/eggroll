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

package com.webank.eggroll.rollpair

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErPartition, ErProcessor, ErStore, ErStoreLocator, ErTask}
import com.webank.eggroll.core.session.{ErConf, RuntimeErConf}
import com.webank.eggroll.core.transfer.GrpcTransferClient
import com.webank.eggroll.rollpair.component.RollPairServicer

import scala.collection.JavaConverters._
import java.util

import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.util.Logging
class RollPairContext(val session: ErSession, defaultStoreType:String = StoreTypes.ROLLPAIR_LMDB) extends Logging {
//  StandaloneManager.main(Array("-s",erSession.sessionId, "-p", erSession.cmClient.endpoint.port.toString))

  def getRollEndpoint(): ErEndpoint = null
  def getEggEndpoint(partitionId: Int): ErEndpoint = null

  def routeToEgg(partiton: ErPartition): ErProcessor = session.routeToEgg(partiton)


  def load(namespace:String, name:String, opts: Map[String,String] = Map()): RollPair = {
    val store = ErStore(storeLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = opts.getOrElse(StringConstants.STORE_TYPE, StoreTypes.ROLLPAIR_LMDB),
      totalPartitions = opts.getOrElse(StringConstants.TOTAL_PARTITIONS, "1").toInt,
      partitioner = opts.getOrElse(StringConstants.PARTITIONER, PartitionerTypes.BYTESTRING_HASH),
      serdes = opts.getOrElse(StringConstants.SERDES, SerdesTypes.CLOUD_PICKLE)
    ))
    val loaded = session.cmClient.getOrCreateStore(store)
    new RollPair(loaded, this)
  }

  // todo: partitioner factory depending on string, and mod partition number
  def partitioner(k: Array[Byte], n: Int): Int = {
    ByteString.copyFrom(k).hashCode() % n
  }
  def getPartitionProcessor(id:Int): ErProcessor = {
    ErProcessor(commandEndpoint = ErEndpoint("localhost", 20001), transferEndpoint = ErEndpoint("localhost", 20001))
  }
}

class RollPair(val store: ErStore, val ctx:RollPairContext, val opts: Map[String,String] = Map()) extends Logging {
  // todo: 1. consider recv-side shuffle; 2. pull up rowPairDb logic; 3. add partition calculation based on session logic;
  def putBatch(broker: Broker[ByteString], opts: util.Map[String, String] = Map[String, String]().asJava): Unit = {
    val totalPartitions = store.storeLocator.totalPartitions
    val transferClients = new Array[GrpcTransferClient](totalPartitions)
    val brokers = new Array[Broker[ByteString]](totalPartitions)

    val job = ErJob(id = "1",
      name = RollPairServicer.putAll,
      inputs = Array(store),
      outputs = Array(store),
      functors = Array.empty,
      options = Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId))

    new Thread {
      override def run(): Unit = {
        val commandClient = new CommandClient(defaultEndpoint = ErEndpoint("localhost", 4670))
        commandClient.call(new CommandURI(RollPairServicer.rollRunJobCommand), job)

        logInfo("thread started")
      }
    }.start()

    // todo: create RowPairDB
    while (!broker.isClosable()) {
      val rowPairDB = broker.poll(10, TimeUnit.SECONDS)

      if (rowPairDB != null) {
        val magicNumber = new Array[Byte](4)
        val protocolVersion = new Array[Byte](4)

        val byteBuffer: ByteBuffer = rowPairDB.asReadOnlyByteBuffer()
        byteBuffer.order(ByteOrder.BIG_ENDIAN)
        byteBuffer.get(magicNumber)
        byteBuffer.get(protocolVersion)

        if (!magicNumber.sameElements(NetworkConstants.TRANSFER_PROTOCOL_MAGIC_NUMBER)) {
          throw new IllegalArgumentException("transfer protocol magic number not match")
        }

        if (!protocolVersion.sameElements(NetworkConstants.TRANSFER_PROTOCOL_VERSION)) {
          throw new IllegalArgumentException("protocol not supported")
        }

        val headerSize = byteBuffer.getInt
        if (headerSize > 0) {
          // todo: process header > 0
        }

        val bodySize = byteBuffer.getInt()

        val kLen = byteBuffer.getInt()
        val k = new Array[Byte](kLen)
        byteBuffer.get(k)

        val partitionId = ctx.partitioner(k, totalPartitions)

        if (transferClients(partitionId) == null) {
          val newBroker = new LinkedBlockingBroker[ByteString]()
          brokers.update(partitionId, newBroker)

          val newTransferClient = new GrpcTransferClient()
          // val proc = ErProcessor(commandEndpoint = ErEndpoint("localhost",20001))
          newTransferClient.initForward(
            dataBroker = newBroker,
            tag = s"${job.id}-${partitionId}",
            processor = ctx.routeToEgg(store.partitions(partitionId)))
          transferClients.update(partitionId, newTransferClient)
        }

        val transferClient = transferClients(partitionId)

        brokers(partitionId).put(rowPairDB)
        transferClient.doSend()

        /* send putBatch command*/
        //val storeLocator = ErStoreLocator(StoreTypes.ROLLPAIR_LEVELDB, "ns", "name")
/*        val rollPair = new RollPairServicer()

        rollPair.putBatch(job)*/
      }
    }

    transferClients.foreach(c => c.complete())

  }
}

