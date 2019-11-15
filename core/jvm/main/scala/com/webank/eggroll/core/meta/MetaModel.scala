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

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.meta.NetworkingModelPbSerdes._
import com.webank.eggroll.core.serdes.{PbMessageDeserializer, PbMessageSerializer}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

case class ErFunctor(name: String = StringConstants.EMPTY,
                     serdes: String = StringConstants.EMPTY,
                     body: Array[Byte]) extends RpcMessage

case class ErPair(key: Array[Byte], value: Array[Byte]) extends RpcMessage

case class ErPairBatch(pairs: Array[ErPair]) extends RpcMessage

case class ErStoreLocator(storeType: String,
                          namespace: String,
                          name: String,
                          path: String = StringConstants.EMPTY,
                          totalPartitions: Int = 0,
                          partitioner: String = StringConstants.EMPTY,
                          serdes: String = StringConstants.EMPTY) extends RpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = {
    if (!StringUtils.isBlank(path)) {
      path
    } else {
      String.join(delim, storeType, namespace, name)
    }
  }

  def fork(postfix: String = StringConstants.EMPTY, delimiter: String = StringConstants.UNDERLINE): ErStoreLocator = {
    this.copy(name = if (StringUtils.isBlank(postfix)) System.nanoTime().toString else postfix)
  }
}

case class ErPartition(id: Int, storeLocator: ErStoreLocator, processor: ErProcessor) extends RpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = String.join(delim, storeLocator.toPath(delim = delim), id.toString)
}

case class ErStore(storeLocator: ErStoreLocator, partitions: Array[ErPartition] = Array.empty) extends RpcMessage {
  def toPath(delim: String = StringConstants.SLASH): String = storeLocator.toPath(delim = delim)

  def fork(storeLocator: ErStoreLocator): ErStore = {
    val finalStoreLocator = if (storeLocator == null) storeLocator.fork() else storeLocator

    ErStore(storeLocator = finalStoreLocator, partitions = partitions.map(p => p.copy(storeLocator = finalStoreLocator)))
  }

  def fork(postfix: String = StringConstants.EMPTY, delimiter: String = StringConstants.UNDERLINE): ErStore = {
    fork(storeLocator = storeLocator.fork(postfix = postfix, delimiter = delimiter))
  }
}

case class ErJob(id: String, name: String = StringConstants.EMPTY, inputs: Array[ErStore], outputs: Array[ErStore] = Array(), functors: Array[ErFunctor]) extends RpcMessage

case class ErTask(id: String, name: String = StringConstants.EMPTY, inputs: Array[ErPartition], outputs: Array[ErPartition], job: ErJob) extends RpcMessage {
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

/*object MetaModelUtils {
  def forkInputs(inputs: List[ErStore],
                 postfix: String = StringConstants.EMPTY,
                 delimiter: String = StringConstants.UNDERLINE): List[ErStore] = {
    inputs.map(p => p.fork(postfix = postfix, delimiter = delimiter))
  }
}*/

object MetaModelPbSerdes {

  // serializers
  implicit class ErFunctorToPbMessage(src: ErFunctor) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Functor = {
      val builder = Meta.Functor.newBuilder()
        .setName(src.name)
        .setSerdes(src.serdes)
        .setBody(ByteString.copyFrom(src.body))

      builder.build()
    }
  }

  implicit class ErPairToPbMessage(src: ErPair) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Pair = {
      val builder = Meta.Pair.newBuilder()
        .setKey(ByteString.copyFrom(src.key))
        .setValue(ByteString.copyFrom(src.value))

      builder.build()
    }
  }

  implicit class ErPairBatchToPbMessage(src: ErPairBatch) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.PairBatch = {
      val builder = Meta.PairBatch.newBuilder()
        .addAllPairs(src.pairs.toList.map(_.toProto()).asJava)

      builder.build()
    }
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
  }

  implicit class ErStoreToPbMessage(src: ErStore) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Store = {
      val builder = Meta.Store.newBuilder()
        .setStoreLocator(src.storeLocator.toProto())
        .addAllPartitions(src.partitions.toList.map(_.toProto()).asJava)

      builder.build()
    }
  }

  implicit class ErPartitionToPbMessage(src: ErPartition) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Partition = {
      val builder = Meta.Partition.newBuilder()
        .setId(src.id)
        .setStoreLocator(src.storeLocator.toProto())
        .setProcessor(src.processor.toProto())

      builder.build()
    }
  }

  implicit class ErJobToPbMessage(src: ErJob) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Job = {
      val builder = Meta.Job.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllInputs(src.inputs.toList.map(_.toProto()).asJava)
        .addAllOutputs(src.outputs.toList.map(_.toProto()).asJava)
        .addAllFunctors(src.functors.toList.map(_.toProto()).asJava)

      builder.build()
    }
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
  }

  // deserializers
  implicit class ErFunctorFromPbMessage(src: Meta.Functor) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErFunctor = {
      ErFunctor(name = src.getName, serdes = src.getSerdes, body = src.getBody.toByteArray)
    }
  }

  implicit class ErPairFromPbMessage(src: Meta.Pair) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPair = {
      ErPair(key = src.getKey.toByteArray, value = src.getValue.toByteArray)
    }
  }

  implicit class ErPairBatchFromPbMessage(src: Meta.PairBatch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPairBatch = {
      ErPairBatch(pairs = src.getPairsList.asScala.map(_.fromProto()).toArray)
    }
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
  }

  implicit class ErStoreFromPbMessage(src: Meta.Store) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStore = {
      ErStore(storeLocator = src.getStoreLocator.fromProto(), src.getPartitionsList.asScala.map(_.fromProto()).toArray)
    }
  }

  implicit class ErPartitionFromPbMessage(src: Meta.Partition) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPartition = {
      ErPartition(id = src.getId, storeLocator = src.getStoreLocator.fromProto(), processor = src.getProcessor.fromProto())
    }
  }

  implicit class ErJobFromPbMessage(src: Meta.Job) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErJob = {
      ErJob(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toArray,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toArray,
        functors = src.getFunctorsList.asScala.map(_.fromProto()).toArray)
    }
  }

  implicit class ErTaskFromPbMessage(src: Meta.Task) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTask = {
      ErTask(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toArray,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toArray,
        job = src.getJob.fromProto())
    }
  }

}
