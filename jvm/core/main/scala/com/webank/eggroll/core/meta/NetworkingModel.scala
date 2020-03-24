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

import java.util.concurrent.ConcurrentHashMap

import com.google.protobuf.{Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.serdes.{BaseSerializable, PbMessageDeserializer, PbMessageSerializer}
import jdk.nashorn.internal.ir.annotations.Immutable
import org.apache.commons.lang3.StringUtils

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

trait NetworkingRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "Networking"
}

@Immutable
case class ErEndpoint(@BeanProperty host: String, @BeanProperty port: Int = -1) extends NetworkingRpcMessage {
  override def toString: String = s"$host:$port"

  def isValid: Boolean = !StringUtils.isBlank(host) && port > 0
}
object ErEndpoint {
  def apply(url: String): ErEndpoint = {
    val toks = url.split(":")
    new ErEndpoint(toks(0), toks(1).toInt)
  }
}

case class ErProcessor(id: Long = -1,
                       serverNodeId: Long = -1,
                       name: String = StringConstants.EMPTY,
                       processorType: String = StringConstants.EMPTY,
                       status: String = StringConstants.EMPTY,
                       commandEndpoint: ErEndpoint = null,
                       transferEndpoint: ErEndpoint = null,
                       pid: Int = -1,
                       options: java.util.Map[String, String] = new ConcurrentHashMap[String, String](),
                       tag: String = StringConstants.EMPTY) extends NetworkingRpcMessage {
  override def toString: String = {
    s"<ErProcessor(id=${id}, serverNodeId=${serverNodeId}, name=${name}, processorType=${processorType}, status=${status}, commandEndpoint=${commandEndpoint}, transferEndpoint=${transferEndpoint}, pid=${pid}, options=${options}, tag=${tag}) at ${hashCode().toHexString}>"
  }
}

case class ErProcessorBatch(id: Long = -1,
                            name: String = StringConstants.EMPTY,
                            processors: Array[ErProcessor] = Array(),
                            tag: String = StringConstants.EMPTY) extends NetworkingRpcMessage

case class ErServerNode(id: Long = -1,
                        name: String = StringConstants.EMPTY,
                        clusterId: Long = 0,
                        endpoint: ErEndpoint = ErEndpoint(host = StringConstants.EMPTY, port = -1),
                        nodeType: String = StringConstants.EMPTY,
                        status: String = StringConstants.EMPTY) extends NetworkingRpcMessage

case class ErServerCluster(id: Long = -1,
                           name: String = StringConstants.EMPTY,
                           serverNodes: Array[ErServerNode] = Array(),
                           tag: String = StringConstants.EMPTY) extends NetworkingRpcMessage

object NetworkingModelPbMessageSerdes {

  // serializers
  implicit class ErEndpointToPbMessage(src: ErEndpoint) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Endpoint = {
      val builder = Meta.Endpoint.newBuilder()
        .setHost(src.host)
        .setPort(src.port)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErEndpoint].toBytes()
  }

  implicit class ErProcessorToPbMessage(src: ErProcessor) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Processor = {
      val builder = Meta.Processor.newBuilder()
        .setId(src.id)
        .setServerNodeId(src.serverNodeId)
        .setName(src.name)
        .setProcessorType(src.processorType)
        .setStatus(src.status)
        .setCommandEndpoint(if (src.commandEndpoint != null ) src.commandEndpoint.toProto() else Meta.Endpoint.getDefaultInstance)
        .setTransferEndpoint(if (src.transferEndpoint != null) src.transferEndpoint.toProto() else Meta.Endpoint.getDefaultInstance)
        .setPid(src.pid)
        .putAllOptions(src.options)
        .setTag(src.tag)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErProcessor].toBytes()
  }

  implicit class ErProcessorBatchToPbMessage(src: ErProcessorBatch) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.ProcessorBatch = {
      val builder = Meta.ProcessorBatch.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .addAllProcessors(src.processors.toList.map(_.toProto()).asJava)
        .setTag(src.tag)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErProcessorBatch].toBytes()
  }

  implicit class ErServerNodeToPbMessage(src: ErServerNode) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.ServerNode = {
      val builder = Meta.ServerNode.newBuilder()
        .setId(src.id)
        .setName(src.name)
        .setClusterId(src.clusterId)
        .setEndpoint(src.endpoint.toProto())
        .setNodeType(src.nodeType)
        .setStatus(src.status)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErServerNode].toBytes()
  }

  implicit class ErServerClusterToPbMessage(src: ErServerCluster) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.ServerCluster = {
      val builder = Meta.ServerCluster.newBuilder()
        .setId(src.id)
        .addAllServerNodes(src.serverNodes.toList.map(_.toProto()).asJava)
        .setTag(src.tag)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErServerCluster].toBytes()
  }

  // deserializers
  implicit class ErEndpointFromPbMessage(src: Meta.Endpoint = null) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErEndpoint = {
      ErEndpoint(host = src.getHost, port = src.getPort)
    }

    override def fromBytes(bytes: Array[Byte]): ErEndpoint =
      Meta.Endpoint.parseFrom(bytes).fromProto()
  }

  implicit class ErProcessorFromPbMessage(src: Meta.Processor) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErProcessor = {
      ErProcessor(
        id = src.getId,
        serverNodeId = src.getServerNodeId,
        name = src.getName,
        processorType = src.getProcessorType,
        status = src.getStatus,
        commandEndpoint = src.getCommandEndpoint.fromProto(),
        transferEndpoint = src.getTransferEndpoint.fromProto(),
        pid = src.getPid,
        options = src.getOptionsMap,
        tag = src.getTag)
    }

    override def fromBytes(bytes: Array[Byte]): ErProcessor =
      Meta.Processor.parseFrom(bytes).fromProto()
  }

  implicit class ErProcessorBatchFromPbMessage(src: Meta.ProcessorBatch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErProcessorBatch = {
      ErProcessorBatch(
        id = src.getId,
        name = src.getName,
        processors = src.getProcessorsList.asScala.map(_.fromProto()).toArray,
        tag = src.getTag)
    }

    override def fromBytes(bytes: Array[Byte]): ErProcessorBatch =
      Meta.ProcessorBatch.parseFrom(bytes).fromProto()
  }

  implicit class ErServerNodeFromPbMessage(src: Meta.ServerNode) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErServerNode = {
      ErServerNode(
        id = src.getId,
        name = src.getName,
        clusterId = src.getClusterId,
        endpoint = src.getEndpoint.fromProto(),
        nodeType = src.getNodeType,
        status = src.getStatus)
    }

    override def fromBytes(bytes: Array[Byte]): ErServerNode =
      Meta.ServerNode.parseFrom(bytes).fromProto()
  }

  implicit class ErServerClusterFromPbMessage(src: Meta.ServerCluster) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErServerCluster =
      ErServerCluster(
        id = src.getId,
        serverNodes = src.getServerNodesList.asScala.map(_.fromProto()).toArray,
        tag = src.getTag)

    override def fromBytes(bytes: Array[Byte]): ErServerCluster =
      Meta.ServerCluster.parseFrom(bytes).fromProto()
  }
}
