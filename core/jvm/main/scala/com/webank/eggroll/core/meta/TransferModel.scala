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

case class ErBatchId(id: String) extends RpcMessage

case class ErBatch(id: ErBatchId, data: Array[Byte]) extends RpcMessage

object TransferModelPbSerdes {

  // serializers
  implicit class ErBatchIdToPbMessage(src: ErBatchId) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.BatchId = {
      val builder = Transfer.BatchId.newBuilder()
        .setId(src.id)

      builder.build()
    }
  }

  implicit class ErBatchToPbMessage(src: ErBatch) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.Batch = {
      val builder = Transfer.Batch.newBuilder()
        .setId(src.id.toProto())
        .setData(ByteString.copyFrom(src.data))
        .setSize(src.data.size)

      builder.build()
    }
  }

  // deserializers
  implicit class ErBatchIdFromPbMessage(src: Transfer.BatchId) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErBatchId = {
      ErBatchId(id = src.getId)
    }
  }

  implicit class ErBatchFromPbMessage(src: Transfer.Batch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErBatch = {
      ErBatch(id = src.getId.fromProto(), data = src.getData.toByteArray)
    }
  }

}

