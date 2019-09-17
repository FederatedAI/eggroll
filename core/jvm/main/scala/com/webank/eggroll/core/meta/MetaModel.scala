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
 */

package com.webank.eggroll.core.meta

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.NetworkingModelPbSerdes._
import com.webank.eggroll.core.rpc.RpcMessage
import com.webank.eggroll.core.serdes.{PbMessageDeserializer, PbMessageSerializer}

import scala.collection.JavaConverters._

case class ErFunctor(name: String = StringConstants.EMPTY, body: Array[Byte]) extends RpcMessage

case class ErStoreLocator(storeType: String, namespace: String, name: String, path: String = StringConstants.EMPTY) extends RpcMessage

case class ErStore(storeLocator: ErStoreLocator, partitions: List[ErPartition] = List.empty) extends RpcMessage

case class ErPartition(id: String, storeLocator: ErStoreLocator, node: ErServerNode) extends RpcMessage

case class ErJob(id: String, name: String = StringConstants.EMPTY, inputs: List[ErStore], outputs: List[ErStore] = List(), functors: List[ErFunctor]) extends RpcMessage

case class ErTask(id: String, name: String = StringConstants.EMPTY, inputs: List[ErPartition], outputs: List[ErPartition], job: ErJob) extends RpcMessage {
  def getEndpoint: ErEndpoint = {
    if (inputs == null || inputs.isEmpty) {
      throw new IllegalArgumentException("Partition input is empty")
    }

    val node = inputs.head.node

    if (node == null) {
      throw new IllegalArgumentException("Head node's input partition is null")
    }

    node.endpoint
  }
}

object MetaModelPbSerdes {

  // serializers
  implicit class ErFunctorToPbMessage(src: ErFunctor) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Functor = {
      val builder = Meta.Functor.newBuilder()
        .setName(src.name)
        .setBody(ByteString.copyFrom(src.body))

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

      builder.build()
    }
  }

  implicit class ErStoreToPbMessage(src: ErStore) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Store = {
      val builder = Meta.Store.newBuilder()
        .setStoreLocator(src.storeLocator.toProto())
        .addAllPartition(src.partitions.map(_.toProto()).asJava)

      builder.build()
    }
  }

  implicit class ErPartitionToPbMessage(src: ErPartition) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Partition = {
      val builder = Meta.Partition.newBuilder()
        .setId(src.id)
        .setStoreLocator(src.storeLocator.toProto())
        .setNode(src.node.toProto())

      builder.build()
    }
  }

  implicit class ErJobToPbMessage(src: ErJob) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Job = {
      val builder = Meta.Job.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllInputs(src.inputs.map(_.toProto()).asJava)
        .addAllOutputs(src.outputs.map(_.toProto()).asJava)
        .addAllFunctors(src.functors.map(_.toProto()).asJava)

      builder.build()
    }
  }

  implicit class ErTaskToPbMessage(src: ErTask) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Task = {
      val builder = Meta.Task.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllInputs(src.inputs.map(_.toProto()).asJava)
        .addAllOutputs(src.outputs.map(_.toProto()).asJava)
        .setJob(src.job.toProto())

      builder.build()
    }
  }

  // deserializers
  implicit class ErFunctorFromPbMessage(src: Meta.Functor) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErFunctor = {
      ErFunctor(name = src.getName, body = src.getBody.toByteArray)
    }
  }

  implicit class ErStoreLocatorFromPbMessage(src: Meta.StoreLocator) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStoreLocator = {
      ErStoreLocator(storeType = src.getStoreType, namespace = src.getNamespace, name = src.getName, path = src.getPath)
    }
  }

  implicit class ErStoreFromPbMessage(src: Meta.Store) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErStore = {
      ErStore(storeLocator = src.getStoreLocator.fromProto(), src.getPartitionList.asScala.map(_.fromProto()).toList)
    }
  }

  implicit class ErPartitionFromPbMessage(src: Meta.Partition) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErPartition = {
      ErPartition(id = src.getId, storeLocator = src.getStoreLocator.fromProto(), node = src.getNode.fromProto())
    }
  }

  implicit class ErJobFromPbMessage(src: Meta.Job) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErJob = {
      ErJob(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toList,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toList,
        functors = src.getFunctorsList.asScala.map(_.fromProto()).toList)
    }
  }

  implicit class ErTaskFromPbMessage(src: Meta.Task) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTask = {
      ErTask(id = src.getId,
        name = src.getName,
        inputs = src.getInputsList.asScala.map(_.fromProto()).toList,
        outputs = src.getOutputsList.asScala.map(_.fromProto()).toList,
        job = src.getJob.fromProto())
    }
  }

}
