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

import com.google.protobuf.{Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.rpc.RpcMessage
import com.webank.eggroll.core.serdes.{PbMessageDeserializer, PbMessageSerializer}
import jdk.nashorn.internal.ir.annotations.Immutable

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

@Immutable
case class ErEndpoint(@BeanProperty host: String, @BeanProperty port: Int) extends RpcMessage {
  override def toString: String = s"$host:$port"
}

case class ErServerNode(id: String = StringConstants.EMPTY, endpoint: ErEndpoint = null, tag: String = StringConstants.EMPTY) extends RpcMessage

case class ErServerCluster(id: String = StringConstants.EMPTY, nodes: List[ErServerNode] = List()) extends RpcMessage

object NetworkingModelPbSerdes {

  // serializers
  implicit class ErEndpointToPbMessage(src: ErEndpoint) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.Endpoint = {
      val builder = Meta.Endpoint.newBuilder()
        .setHost(src.host)
        .setPort(src.port)

      builder.build()
    }
  }

  implicit class ErServerNodeToPbMessage(src: ErServerNode) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.ServerNode = {
      val builder = Meta.ServerNode.newBuilder()
        .setId(src.id)
        .setEndpoint(src.endpoint.toProto())
        .setTag(src.tag)

      builder.build()
    }
  }

  implicit class ErServerClusterToPbMessage(src: ErServerCluster) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Meta.ServerCluster = {
      val builder = Meta.ServerCluster.newBuilder()
        .setId(src.id)

      val serializedNodes = src.nodes.map(_.toProto())

      builder.addAllNodes(serializedNodes.asJava)

      builder.build()
    }
  }

  // deserializers
  implicit class ErEndpointFromPbMessage(src: Meta.Endpoint) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErEndpoint = {
      ErEndpoint(host = src.getHost, port = src.getPort)
    }
  }

  implicit class ErServerNodeFromPbMessage(src: Meta.ServerNode) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErServerNode = {
      ErServerNode(id = src.getId, endpoint = src.getEndpoint.fromProto(), tag = src.getTag)
    }
  }

  implicit class ErServerClusterFromPbMessage(src: Meta.ServerCluster) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErServerCluster = {
      val deserializedNodes = src.getNodesList.asScala.map(_.fromProto()).toList

      ErServerCluster(id = src.getId, nodes = deserializedNodes)
    }
  }

}
