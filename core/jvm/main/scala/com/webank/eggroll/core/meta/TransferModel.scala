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

import com.google.protobuf.{ByteString, Message}
import com.webank.eggroll.core.rpc.RpcMessage
import com.webank.eggroll.core.serdes.{PbMessageDeserializer, PbMessageSerializer}
import com.webank.eggroll.core.transfer.Transfer

case class ErTransferHeader(id: Int, tag: String, totalSize: Long) extends RpcMessage

case class ErBatch(header: ErTransferHeader, data: Array[Byte]) extends RpcMessage

object TransferModelPbSerdes {

  // serializers
  implicit class ErTransferHeaderToPbMessage(src: ErTransferHeader) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.TransferHeader = {
      val builder = Transfer.TransferHeader.newBuilder()
        .setId(src.id)
        .setTag(src.tag)
        .setTotalSize(src.totalSize)

      builder.build()
    }
  }

  implicit class ErBatchToPbMessage(src: ErBatch) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.Batch = {
      val builder = Transfer.Batch.newBuilder()
        .setHeader(src.header.toProto())
        .setData(ByteString.copyFrom(src.data))
        .setBatchSize(src.data.size)

      builder.build()
    }
  }

  // deserializers
  implicit class ErBatchIdFromPbMessage(src: Transfer.TransferHeader) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTransferHeader = {
      ErTransferHeader(id = src.getId, tag = src.getTag, totalSize = src.getTotalSize)
    }
  }

  implicit class ErBatchFromPbMessage(src: Transfer.Batch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErBatch = {
      ErBatch(header = src.getHeader.fromProto(), data = src.getData.toByteArray)
    }
  }
}

