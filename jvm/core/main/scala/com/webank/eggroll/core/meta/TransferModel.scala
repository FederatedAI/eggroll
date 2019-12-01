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

import com.google.protobuf.{ByteString, Message}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.serdes.{BaseSerializable, PbMessageDeserializer, PbMessageSerializer}
import com.webank.eggroll.core.transfer.Transfer

trait TransferRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "Transfer"
}

case class ErTransferHeader(id: Int, tag: String, totalSize: Long, status: String = StringConstants.EMPTY) extends TransferRpcMessage

case class ErTransferBatch(header: ErTransferHeader, data: Array[Byte]) extends TransferRpcMessage

object TransferModelPbMessageSerdes {

  // serializers
  implicit class ErTransferHeaderToPbMessage(src: ErTransferHeader) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.TransferHeader = {
      val builder = Transfer.TransferHeader.newBuilder()
        .setId(src.id)
        .setTag(src.tag)
        .setTotalSize(src.totalSize)
        .setStatus(src.status)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErTransferHeader].toBytes()
  }

  implicit class ErBatchToPbMessage(src: ErTransferBatch) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.TransferBatch = {
      val builder = Transfer.TransferBatch.newBuilder()
        .setHeader(src.header.toProto())
        .setData(ByteString.copyFrom(src.data))
        .setBatchSize(src.data.size)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErTransferBatch].toBytes()
  }

  // deserializers
  implicit class ErBatchIdFromPbMessage(src: Transfer.TransferHeader) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTransferHeader = {
      ErTransferHeader(id = src.getId, tag = src.getTag, totalSize = src.getTotalSize, status = src.getStatus)
    }

    override def fromBytes(bytes: Array[Byte]): ErTransferHeader =
      Transfer.TransferHeader.parseFrom(bytes).fromProto()
  }

  implicit class ErBatchFromPbMessage(src: Transfer.TransferBatch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTransferBatch = {
      ErTransferBatch(header = src.getHeader.fromProto(), data = src.getData.toByteArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErTransferHeader =
      Transfer.TransferHeader.parseFrom(bytes).fromProto()
  }
}

