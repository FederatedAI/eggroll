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

package com.webank.eggroll.rollpair.client

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.meta.{ErJob, ErStore, ErStoreLocator}
import com.webank.eggroll.core.session.{ErConf, RuntimeErConf}
import com.webank.eggroll.core.transfer.GrpcTransferClient
import com.webank.eggroll.rollpair.component.RollPairServicer

class RollPair(val store: ErStore, val opts: ErConf = RuntimeErConf()) {
  private var __store: ErStore = null
  private val clusterManagerHost = opts.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, "localhost")
  private val clusterManagerPort = opts.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, 4670)

  private val clusterManagerClient = new ClusterManagerClient(clusterManagerHost, clusterManagerPort)

  land(store, opts)

  def land(store: ErStore, opts: ErConf = RuntimeErConf()): RollPair = {
    var finalStore = store
    if (finalStore == null) {
      finalStore = ErStore(storeLocator = ErStoreLocator(
        storeType = opts.getString(StringConstants.STORE_TYPE, StoreTypes.ROLLPAIR_LEVELDB),
        namespace = opts.getString(StringConstants.NAMESPACE),
        name = opts.getString(StringConstants.NAME),
        totalPartitions = opts.getInt(StringConstants.TOTAL_PARTITIONS, 0),
        partitioner = opts.getString(StringConstants.PARTITIONER, PartitionerTypes.BYTESTRING_HASH),
        serdes = opts.getString(StringConstants.SERDES, SerdesTypes.CLOUD_PICKLE)
      ))
    }

    __store = clusterManagerClient.getOrCreateStore(finalStore)

    this
  }

  // todo: 1. consider recv-side shuffle; 2. pull up rowPairDb logic; 3. add partition calculation based on session logic;
  def putBatch(broker: Broker[ByteString], output: ErStore = null, opts: ErConf = RuntimeErConf()): RollPair = {
    val totalPartitions = __store.storeLocator.totalPartitions
    val transferClients = new Array[GrpcTransferClient](totalPartitions)
    val brokers = new Array[Broker[ByteString]](totalPartitions)
    // todo: partitioner factory depending on string, and mod partition number
    def partitioner(k: Array[Byte], n: Int): Int = {
      ByteString.copyFrom(k).hashCode() % n
    }

    // todo: create RowPairDB
    while (!broker.isClosable()) {
      val rowPairDB = broker.poll(10, TimeUnit.SECONDS)

      if (rowPairDB != null) {
        val magicNumber = new Array[Byte](8)
        val protocolVersion = new Array[Byte](4)

        val byteBuffer: ByteBuffer = rowPairDB.asReadOnlyByteBuffer()
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

        val partitionId = partitioner(k, totalPartitions)

        if (transferClients(partitionId) == null) {
          val newBroker = new LinkedBlockingBroker[ByteString]()
          brokers.update(partitionId, newBroker)
          val newTransferClient = new GrpcTransferClient()

          newBroker.put(rowPairDB)
          newTransferClient.initForward(dataBroker = newBroker, tag = s"forward-${partitionId}", processor = __store.partitions(partitionId).processor)
          transferClients.update(partitionId, newTransferClient)
        }

        val transferClient = transferClients(partitionId)

        transferClient.doSend()

        /* send putBatch command*/
        val storeLocator = ErStoreLocator(StoreTypes.ROLLPAIR_LEVELDB, "ns", "name")
        val rollPair = new RollPairServicer()
        val job = ErJob(id = "1",
          name = "putBatch",
          inputs = Array(ErStore(storeLocator)),
          functors = Array())
        val result = rollPair.putBatch(job)
      }
    }

    transferClients.foreach(c => c.complete())

    this
  }
}
