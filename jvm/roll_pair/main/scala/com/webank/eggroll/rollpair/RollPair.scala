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

import java.nio.charset.StandardCharsets
import java.util.concurrent.{Callable, Future, ThreadPoolExecutor}

import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.datastructure.FifoBroker
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.transfer.{InternalTransferClient, Transfer}
import com.webank.eggroll.core.util.{IdUtils, Logging, ThreadPoolUtils}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.JavaConverters._


class RollPairContext(val session: ErSession,
                      defaultStoreType: String = RollPairConfKeys.EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE.get(),
                      defaultSerdesType: String = SerdesTypes.PICKLE) extends Logging {
//  StandaloneManager.main(Array("-s",erSession.sessionId, "-p", erSession.cmClient.endpoint.port.toString))
  private val sessionId = session.sessionId
  private val sessionMeta = session.sessionMeta

  def routeToEgg(partition: ErPartition): ErProcessor = session.routeToEgg(partition)

  def load(namespace: String, name: String, options: Map[String,String] = Map()): RollPair = {
    // TODO:1: use snake case universally?
    val defaultStoreTypeValue = defaultStoreType.split("_")(1)
    val storeType = options.getOrElse(StringConstants.STORE_TYPE, options.getOrElse(StringConstants.STORE_TYPE_SNAKECASE, defaultStoreTypeValue))
    val totalPartitions = options.getOrElse(StringConstants.TOTAL_PARTITIONS, options.getOrElse(StringConstants.TOTAL_PARTITIONS_SNAKECASE, "1")).toInt
    val store = ErStore(storeLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = storeType,
      totalPartitions = totalPartitions,
      partitioner = options.getOrElse(StringConstants.PARTITIONER, PartitionerTypes.BYTESTRING_HASH),
      serdes = options.getOrElse(StringConstants.SERDES, defaultSerdesType)
    ), options = options.asJava)
    val loaded = session.clusterManagerClient.getOrCreateStore(store)
    new RollPair(loaded, this)
  }

  // todo:1: partitioner factory depending on string, and mod partition number
  def partitioner(k: Array[Byte], n: Int): Int = {
    // Integer.MIN_VALUE  ==  Math.abs(Integer.MIN_VALUE)
    hashKey(k) % n
  }
  def hashKey(k: Array[Byte]): Int = {
    var h = Math.abs(ByteString.copyFrom(k).hashCode())
    if (h == Integer.MIN_VALUE) {
      h = 1
    }
    h
  }
}

object RollPairContext {
  val executor: ThreadPoolExecutor = ThreadPoolUtils.newCachedThreadPool("rollpair-context")
}

class RollPair(val store: ErStore,
               val ctx: RollPairContext,
               val options: Map[String,String] = Map.empty) extends Logging {
  def putBatch(packets: Iterator[Proxy.Packet],
              options: Map[String, String] = Map.empty): Unit = {
    val totalPartitions = store.storeLocator.totalPartitions
    val brokers = new Array[FifoBroker[Transfer.TransferBatch]](totalPartitions)
    val commandFutures = new Array[Future[ErTask]](totalPartitions)
    val transferFutures = new Array[Future[Transfer.TransferBatch]](totalPartitions)
    val error = new DistributedRuntimeException()

    val jobId = IdUtils.generateJobId(sessionId = ctx.session.sessionId,
    tag = options.getOrElse("job_id_tag", StringConstants.EMPTY))
    val job = ErJob(id = jobId,
      name = RollPair.PUT_ALL,
      inputs = Array(store),
      outputs = Array(store),
      functors = Array.empty,
      options = options ++ Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId, "fed_transfer" -> "true"))

    val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
    val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()

    for (packet <- packets) {
      val rollSiteHeader = RollSiteHeader.parseFrom(
          packet.getHeader.getTask.getModel.getName.getBytes(
          StandardCharsets.ISO_8859_1)).fromProto()

      val partitionId = rollSiteHeader.options("partition_id").toInt

      val broker = if (transferFutures(partitionId) == null) {
        val partition = store.partitions(partitionId)
        val egg = ctx.session.routeToEgg(partition)
        val task = ErTask(id = IdUtils.generateTaskId(job.id, partitionId, RollPair.PUT_BATCH),
          name = RollPair.PUT_ALL,
          inputs = Array(partition),
          outputs = Array(partition),
          job = job)

        val commandFuture = RollPairContext.executor.submit(new Callable[ErTask] {
          override def call(): ErTask = {
            logTrace(s"thread started for put batch taskId=${task.id}")
            val commandClient = new CommandClient(egg.commandEndpoint)
            val result = commandClient.call[ErTask](RollPair.EGG_RUN_TASK_COMMAND, task)
            logTrace(s"thread ended for put batch taskId=${task.id}")
            result
          }
        })
        commandFutures.update(partitionId, commandFuture)

        val newBroker = new FifoBroker[Transfer.TransferBatch]()
        brokers.update(partitionId, newBroker)

        val internalTransferClient = new InternalTransferClient(egg.transferEndpoint)
        transferFutures.update(partitionId, internalTransferClient.sendAsync(newBroker))

        newBroker
      } else {
        brokers(partitionId)
      }

      val batch = transferBatchBuilder.setHeader(transferHeaderBuilder.setId(packet.getHeader.getSeq.toInt))
        .setData(packet.getBody.getValue)
        .build()

      broker.broker.put(batch)
    }

    brokers.foreach(b => {
      if (b != null) b.signalWriteFinish()
    })

    transferFutures.foreach(f => {
      if (f != null) f.get()
    })

    commandFutures.foreach(f => {
      if (f != null) f.get()
    })
  }


 /* // todo: 1. consider recv-side shuffle; 2. pull up rowPairDb logic; 3. add partition calculation based on session logic;
  def putBatch(broker: Broker[ByteString], options: Map[String, String] = Map.empty): Unit = {
    val totalPartitions = store.storeLocator.totalPartitions
    val brokers = new Array[Broker[ByteString]](totalPartitions)
    val transferClients = new Array[GrpcTransferClient](totalPartitions)
    val putBatchThreads = new Array[Thread](totalPartitions)
    val error = new DistributedRuntimeException()

    val jobId = IdUtils.generateJobId(sessionId = ctx.session.sessionId, tag = options.getOrElse("job_id_tag", StringConstants.EMPTY))

    val job = ErJob(id = jobId,
      name = RollPair.PUT_ALL,
      inputs = Array(store),
      outputs = Array(store),
      functors = Array.empty,
      options = options ++ Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId))
    logTrace(s"put batch job metadata=${job}")


    // todo: create RowPairDB
    while (!broker.isClosable()) {
      val rowPairDB = broker.poll(10, TimeUnit.SECONDS)

      if (rowPairDB != null) {
        if (rowPairDB.isEmpty) {
          logDebug(s"Empty batch in rowPairDB. jobId=${jobId}")
        } else {
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
            logTrace(s"put batch route=${store.storeLocator}, ${new String(Base64.getEncoder.encode(k))}, ${ctx.hashKey(k)}, $totalPartitions, ${ctx.hashKey(k) % totalPartitions}")
            val partitionId = ctx.partitioner(k, totalPartitions)

            val transferClient = if (transferClients(partitionId) == null) {
              val partition = store.partitions(partitionId)
              val egg = ctx.session.routeToEgg(partition)
              val task = ErTask(id = IdUtils.generateTaskId(job.id, partitionId, RollPair.PUT_BATCH),
                name = RollPair.PUT_ALL,
                inputs = Array(partition),
                outputs = Array(partition),
                job = job)

              val putBatchThread = new Thread {
                override def run(): Unit = {
                  logTrace(s"thread started for put batch taskId=${task.id}")
                  val commandClient = new CommandClient(egg.commandEndpoint)
                  commandClient.call[ErTask](RollPair.EGG_RUN_TASK_COMMAND, task)
                  logTrace(s"thread ended for put batch taskId=${task.id}")
                }
              }
              putBatchThread.setName(s"putBatch-${task.id}")
              putBatchThread.setUncaughtExceptionHandler(new RollPairUncaughtExceptionHandler(error, egg))
              putBatchThread.start()

              putBatchThreads.update(partitionId, putBatchThread)

              val newBroker = new LinkedBlockingBroker[ByteString]()
              brokers.update(partitionId, newBroker)
              val newTransferClient = new GrpcTransferClient()

              newTransferClient.initForward(
                dataBroker = newBroker,
                tag = IdUtils.generateTaskId(jobId, partitionId, RollPair.PUT_BATCH),
                processor = egg)

              transferClients.update(partitionId, newTransferClient)

              newTransferClient
            } else {
              transferClients(partitionId)
            }

            brokers(partitionId).put(rowPairDB)
            transferClient.doSend()
          } // else: skip this packet
        }
      }
    }

    brokers.foreach(b => {
      if (b != null) b.signalWriteFinish()
    })

    transferClients.foreach(c => {
      if (c != null) c.complete()
    })

    putBatchThreads.foreach(t => {
      if (t != null) t.join()
    })

    if (!error.checkEmpty()) error.raise()
  }*/
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
  val PUT_BATCH = "putBatch"
  val REDUCE = "reduce"
  val SAMPLE = "sample"
  val SUBTRACT_BY_KEY = "subtractByKey"
  val UNION = "union"

  val EGG_RUN_TASK_COMMAND = new CommandURI(s"${EGG_PAIR_URI_PREFIX}/${RUN_TASK}")
  val ROLL_RUN_JOB_COMMAND = new CommandURI(s"${ROLL_PAIR_URI_PREFIX}/${RUN_JOB}")
}

class RollPairUncaughtExceptionHandler(error: DistributedRuntimeException, egg: ErProcessor) extends Thread.UncaughtExceptionHandler with Logging {
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    val builder = new StringBuilder
    builder.append("dst processor id: ")
      .append(egg.id)
      .append(", dst server node id: ")
      .append(egg.serverNodeId)
      .append(", dst host: ")
      .append(egg.commandEndpoint.host)
      .append(", dst port: ")
      .append(egg.commandEndpoint.port)
    val exceptionWithEggInfo = new RuntimeException(builder.toString, e)
    logError(ExceptionUtils.getStackTrace(exceptionWithEggInfo))
    logError(s"Error in thread ${t.getName}", exceptionWithEggInfo)
    error.append(exceptionWithEggInfo)
    t.interrupt()
  }
}