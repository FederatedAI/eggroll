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

package com.webank.eggroll.core.meta

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes._
import com.webank.eggroll.core.serdes.{BaseSerializable, PbMessageDeserializer, PbMessageSerializer}
import com.webank.eggroll.core.util.TimeUtils
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

trait MetaRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "Meta"
}

case class ErFunctor(name: String = StringConstants.EMPTY,
                     serdes: String = StringConstants.EMPTY,
                     body: Array[Byte]) extends RpcMessage

case class ErPair(key: Array[Byte], value: Array[Byte]) extends MetaRpcMessage

case class ErPairBatch(pairs: Array[ErPair]) extends MetaRpcMessage

case class ErStoreLocator(id: Long = -1L,
                          storeType: String,
                          namespace: String,
                          name: String,
                          path: String = StringConstants.EMPTY,
                          totalPartitions: Int = 0,
                          partitioner: String = StringConstants.EMPTY,
                          serdes: String = StringConstants.EMPTY) extends MetaRpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = {
    if (!StringUtils.isBlank(path)) {
      path
    } else {
      String.join(delim, storeType, namespace, name)
    }
  }
  // TODO:0: replace uuid with simpler human friendly solution
  def fork(postfix: String = StringConstants.EMPTY, delimiter: String = StringConstants.UNDERLINE): ErStoreLocator = {
    val delimiterPos = StringUtils.lastOrdinalIndexOf(this.name, delimiter, 2)

    val newPostfix = if (StringUtils.isBlank(postfix)) String.join(delimiter, TimeUtils.getNowMs(), UUID.randomUUID().toString) else postfix
    val newName =
      if (delimiterPos > 0) s"${StringUtils.substring(this.name, 0, delimiterPos)}${delimiter}${newPostfix}"
      else s"${name}${delimiter}${newPostfix}"

    this.copy(name = newName)
  }
}

case class ErPartition(id: Int,
                       storeLocator: ErStoreLocator = null,
                       processor: ErProcessor = null,
                       rankInNode: Int = -1) extends MetaRpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = String.join(delim, storeLocator.toPath(delim = delim), id.toString)
}

case class ErStore(storeLocator: ErStoreLocator,
                   partitions: Array[ErPartition] = Array.empty,
                   options: java.util.Map[String, String] = new ConcurrentHashMap[String, String]())
  extends MetaRpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = storeLocator.toPath(delim = delim)

  def fork(storeLocator: ErStoreLocator): ErStore = {
    val finalStoreLocator = if (storeLocator == null) storeLocator.fork() else storeLocator

    ErStore(storeLocator = finalStoreLocator, partitions = partitions.map(p => p.copy(storeLocator = finalStoreLocator)))
  }

  def fork(postfix: String = StringConstants.EMPTY, delimiter: String = StringConstants.UNDERLINE): ErStore = {
    fork(storeLocator = storeLocator.fork(postfix = postfix, delimiter = delimiter))
  }
}

case class ErStoreList(stores: Array[ErStore] = Array.empty,
                   options: java.util.Map[String, String] = new ConcurrentHashMap[String, String]())
  extends MetaRpcMessage

case class ErJob(id: String,
                 name: String = StringConstants.EMPTY,
                 inputs: Array[ErStore],
                 outputs: Array[ErStore] = Array(),
                 functors: Array[ErFunctor],
                 options: Map[String, String] = Map[String, String]()) extends MetaRpcMessage

case class ErTask(id: String,
                  name: String = StringConstants.EMPTY,
                  inputs: Array[ErPartition],
                  outputs: Array[ErPartition],
                  job: ErJob) extends MetaRpcMessage {
  def getCommandEndpoint: ErEndpoint = {
    if (inputs == null || inputs.isEmpty) {
      throw new IllegalArgumentException("Partition input is empty")
    }

    val processor = inputs.head.processor

    if (processor == null) {
      throw new IllegalArgumentException("Head node's input partition is null")
    }

    processor.commandEndpoint
  }
}

case class ErSessionMeta(id: String = StringConstants.EMPTY,
                         name: String = StringConstants.EMPTY,
                         status: String = StringConstants.EMPTY,
                         totalProcCount: Int = 0,
                         activeProcCount: Int = 0,
                         tag: String = StringConstants.EMPTY,
                         processors: Array[ErProcessor] = Array(),
                         options: Map[String, String] = Map()) extends MetaRpcMessage {
}

object MetaModelPbMessageSerdes {

  // serializers
  implicit class ErFunctorToPbMessage(src: ErFunctor) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Functor = {
      val builder = Meta.Functor.newBuilder()
        .setName(src.name)
        .setSerdes(src.serdes)
        .setBody(ByteString.copyFrom(src.body))

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErFunctor].toBytes()
  }

  implicit class ErPairToPbMessage(src: ErPair) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Pair = {
      val builder = Meta.Pair.newBuilder()
        .setKey(ByteString.copyFrom(src.key))
        .setValue(ByteString.copyFrom(src.value))

      builder.build()
    }
    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErPair].toBytes()
  }

  implicit class ErPairBatchToPbMessage(src: ErPairBatch) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.PairBatch = {
      val builder = Meta.PairBatch.newBuilder()
        .addAllPairs(src.pairs.toList.map(_.toProto()).asJava)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErPairBatch].toBytes()
  }

  implicit class ErStoreLocatorToPbMessage(src: ErStoreLocator) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.StoreLocator = {
      val builder = Meta.StoreLocator.newBuilder()
        .setStoreType(src.storeType)
        .setNamespace(src.namespace)
        .setName(src.name)
        .setPath(src.path)
        .setTotalPartitions(src.totalPartitions)
        .setPartitioner(src.partitioner)
        .setSerdes(src.serdes)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErStoreLocator].toBytes()
  }

  implicit class ErStoreToPbMessage(src: ErStore) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Store = {
      val builder = Meta.Store.newBuilder()
        .setStoreLocator(src.storeLocator.toProto())
        .addAllPartitions(src.partitions.toList.map(_.toProto()).asJava)
        .putAllOptions(src.options)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErStore].toBytes()
  }

  implicit class ErStoreListToPbMessage(src: ErStoreList) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.StoreList = {
      val builder = Meta.StoreList.newBuilder()
          .addAllStores(src.stores.toList.map(_.toProto()).asJava)
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErStoreList].toBytes()
  }

  implicit class ErPartitionToPbMessage(src: ErPartition) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Partition = {
      val builder = Meta.Partition.newBuilder()
        .setId(src.id)
        .setProcessor(src.processor.toProto())
        .setRankInNode(src.rankInNode)
      if (src.storeLocator != null) builder.setStoreLocator(src.storeLocator.toProto())

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErPartition].toBytes()
  }

  implicit class ErJobToPbMessage(src: ErJob) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Job = {
      val builder = Meta.Job.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllInputs(src.inputs.toList.map(_.toProto()).asJava)
        .addAllOutputs(src.outputs.toList.map(_.toProto()).asJava)
        .addAllFunctors(src.functors.toList.map(_.toProto()).asJava)
        .putAllOptions(src.options.asJava)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErJob].toBytes()
  }

  implicit class ErTaskToPbMessage(src: ErTask) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Task = {
      val builder = Meta.Task.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllInputs(src.inputs.toList.map(_.toProto()).asJava)
        .addAllOutputs(src.outputs.toList.map(_.toProto()).asJava)
        .setJob(src.job.toProto())

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErTask].toBytes()
  }

  implicit class ErSessionMetaToPbMessage(src: ErSessionMeta) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.SessionMeta = {
      val builder = Meta.SessionMeta.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .setStatus(src.status)
        .setTag(src.tag)
        .addAllProcessors(src.processors.toList.map(_.toProto()).asJava)
        .putAllOptions(src.options.asJava)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErSessionMeta].toBytes()
  }

  // deserializers

  implicit class ErFunctorFromPbMessage(src: Meta.Functor) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErFunctor =
      ErFunctor(name = src.getName, serdes = src.getSerdes, body = src.getBody.toByteArray)

    override def fromBytes(bytes: Array[Byte]): ErFunctor =
      Meta.Functor.parseFrom(bytes).fromProto()
  }

  implicit class ErPairFromPbMessage(src: Meta.Pair) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPair =
      ErPair(key = src.getKey.toByteArray, value = src.getValue.toByteArray)

    override def fromBytes(bytes: Array[Byte]): ErPair =
      Meta.Pair.parseFrom(bytes).fromProto()
  }

  implicit class ErPairBatchFromPbMessage(src: Meta.PairBatch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPairBatch = {
      ErPairBatch(pairs = src.getPairsList.asScala.map(_.fromProto()).toArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErPairBatch =
      Meta.PairBatch.parseFrom(bytes).fromProto()
  }

  implicit class ErStoreLocatorFromPbMessage(src: Meta.StoreLocator) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStoreLocator = {
      ErStoreLocator(
        storeType = src.getStoreType,
        namespace = src.getNamespace,
        name = src.getName,
        path = src.getPath,
        totalPartitions = src.getTotalPartitions,
        partitioner = src.getPartitioner,
        serdes = src.getSerdes)
    }

    override def fromBytes(bytes: Array[Byte]): ErStoreLocator =
      Meta.StoreLocator.parseFrom(bytes).fromProto()
  }


  implicit class ErStoreFromPbMessage(src: Meta.Store) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStore = {
      ErStore(
        storeLocator = src.getStoreLocator.fromProto(),
        partitions = src.getPartitionsList.asScala.map(_.fromProto()).toArray,
        options = src.getOptionsMap)
    }

    override def fromBytes(bytes: Array[Byte]): ErStore =
      Meta.Store.parseFrom(bytes).fromProto()
  }

  implicit class ErStoreListFromPbMessage(src: Meta.StoreList) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStoreList = {
      ErStoreList(stores = src.getStoresList.asScala.map(_.fromProto()).toArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErStoreList =
      Meta.StoreList.parseFrom(bytes).fromProto()
  }

  implicit class ErPartitionFromPbMessage(src: Meta.Partition) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPartition = {
      ErPartition(id = src.getId, storeLocator = src.getStoreLocator.fromProto(), processor = src.getProcessor.fromProto(), rankInNode = src.getRankInNode)
    }

    override def fromBytes(bytes: Array[Byte]): ErPartition =
      Meta.Partition.parseFrom(bytes).fromProto()
  }

  implicit class ErJobFromPbMessage(src: Meta.Job) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErJob = {
      ErJob(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toArray,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toArray,
        functors = src.getFunctorsList.asScala.map(_.fromProto()).toArray,
        options = src.getOptionsMap.asScala.toMap)
    }

    override def fromBytes(bytes: Array[Byte]): ErJob =
      Meta.Job.parseFrom(bytes).fromProto()
  }

  implicit class ErTaskFromPbMessage(src: Meta.Task) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTask = {
      ErTask(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toArray,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toArray,
        job = src.getJob.fromProto())
    }

    override def fromBytes(bytes: Array[Byte]): ErTask =
      Meta.Task.parseFrom(bytes).fromProto()
  }

  implicit class ErSessionMetaFromPbMessage(src: Meta.SessionMeta) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErSessionMeta = {
      ErSessionMeta(id = src.getId,
        name = src.getName,
        status = src.getStatus,
        tag = src.getTag,
        processors = src.getProcessorsList.asScala.map(_.fromProto()).toArray,
        options = src.getOptionsMap.asScala.toMap)
    }

    override def fromBytes(bytes: Array[Byte]): ErSessionMeta = {
      Meta.SessionMeta.parseFrom(bytes).fromProto()
    }
  }
}
