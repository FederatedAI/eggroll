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
import java.util
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.transfer.GrpcTransferClient
import com.webank.eggroll.core.util.{IdUtils, Logging}

import scala.collection.JavaConverters._
class RollPairContext(val session: ErSession,
                      defaultStoreType: String = StoreTypes.ROLLPAIR_LMDB,
                      defaultSerdesType: String = SerdesTypes.PICKLE) extends Logging {
//  StandaloneManager.main(Array("-s",erSession.sessionId, "-p", erSession.cmClient.endpoint.port.toString))
  private val sessionId = session.sessionId
  private val sessionMeta = session.sessionMeta

  def routeToEgg(partition: ErPartition): ErProcessor = session.routeToEgg(partition)

  def load(namespace: String, name: String, options: Map[String,String] = Map()): RollPair = {
    val store = ErStore(storeLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = options.getOrElse(StringConstants.STORE_TYPE, defaultStoreType),
      totalPartitions = options.getOrElse(StringConstants.TOTAL_PARTITIONS, "1").toInt,
      partitioner = options.getOrElse(StringConstants.PARTITIONER, PartitionerTypes.BYTESTRING_HASH),
      serdes = options.getOrElse(StringConstants.SERDES, defaultSerdesType)
    ))
    val loaded = session.clusterManagerClient.getOrCreateStore(store)
    new RollPair(loaded, this)
  }

  // todo:1: partitioner factory depending on string, and mod partition number
  def partitioner(k: Array[Byte], n: Int): Int = {
    ByteString.copyFrom(k).hashCode() % n
  }
}

class RollPair(val store: ErStore, val ctx: RollPairContext, val options: Map[String,String] = Map()) extends Logging {
  // todo: 1. consider recv-side shuffle; 2. pull up rowPairDb logic; 3. add partition calculation based on session logic;
  def putBatch(broker: Broker[ByteString], opts: util.Map[String, String] = Map[String, String]().asJava): Unit = {
    val totalPartitions = store.storeLocator.totalPartitions
    val transferClients = new Array[GrpcTransferClient](totalPartitions)
    val brokers = new Array[Broker[ByteString]](totalPartitions)

    val jobId = IdUtils.generateJobId(ctx.session.sessionId)
    val job = ErJob(id = jobId,
      name = RollPair.PUT_ALL,
      inputs = Array(store),
      outputs = Array(store),
      functors = Array.empty,
      options = Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId))

    logInfo(s"mw: job: ${job}")
    new Thread {
      override def run(): Unit = {
        val commandClient = new CommandClient(ctx.session.rolls(0).commandEndpoint)
        commandClient.call[ErJob](RollPair.ROLL_RUN_JOB_COMMAND, job)

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

        if (byteBuffer.remaining() > 0) {
          val kLen = byteBuffer.getInt()
          val k = new Array[Byte](kLen)
          byteBuffer.get(k)

          val partitionId = ctx.partitioner(k, totalPartitions)

          if (transferClients(partitionId) == null) {
            val newBroker = new LinkedBlockingBroker[ByteString]()
            brokers.update(partitionId, newBroker)

            val newTransferClient = new GrpcTransferClient()
            // val proc = ErProcessor(commandEndpoint = ErEndpoint("localhost",20001))
            // tag = s"${job.id}-${partitionId}" to store.storeLocator.name
            newTransferClient.initForward(
              dataBroker = newBroker,
              tag = IdUtils.generateTaskId(jobId, partitionId),
              processor = ctx.routeToEgg(store.partitions(partitionId)))
            transferClients.update(partitionId, newTransferClient)
          }

          val transferClient = transferClients(partitionId)

          brokers(partitionId).put(rowPairDB)
          transferClient.doSend()
        } // else: skip this packet
      }
    }

    transferClients.foreach(
      c => if(c!=null) {
        c.complete()
    })

  }
}

object RollPair {
  val ROLL_PAIR_URI_PREFIX = "v1/roll-pair"
  val EGG_PAIR_URI_PREFIX = "v1/egg-pair"

  val RUN_JOB = "runJob"
  val RUN_TASK = "runTask"

  val AGGREGATE = "aggregate"
  val COLLAPSE_PARTITIONS = "collapsePartitions"
  val DELETE = "delete"
  val DESTROY = "destroy"
  val FILTER = "filter"
  val FLAT_MAP = "flatMap"
  val GET = "get"
  val GET_ALL = "getAll"
  val GLOM = "glom"
  val JOIN = "join"
  val MAP = "map"
  val MAP_PARTITIONS = "mapPartitions"
  val MAP_VALUES = "mapValues"
  val PUT = "put"
  val PUT_ALL = "putAll"
  val REDUCE = "reduce"
  val SAMPLE = "sample"
  val SUBTRACT_BY_KEY = "subtractByKey"
  val UNION = "union"

  val EGG_RUN_TASK_COMMAND = new CommandURI(s"${EGG_PAIR_URI_PREFIX}/${RUN_TASK}")
  val ROLL_RUN_JOB_COMMAND = new CommandURI(s"${ROLL_PAIR_URI_PREFIX}/${RUN_JOB}")
}