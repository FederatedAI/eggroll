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

import java.nio.charset.StandardCharsets

import com.google.protobuf.{ByteString, Message}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.serdes.{BaseSerializable, PbMessageDeserializer, PbMessageSerializer}
import com.webank.eggroll.core.transfer.Transfer

import scala.collection.JavaConverters._

trait TransferRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "Transfer"
}

case class ErTransferHeader(id: Int, tag: String, totalSize: Long, status: String = StringConstants.EMPTY, ext: Array[Byte] = Array.emptyByteArray) extends TransferRpcMessage

case class ErTransferBatch(header: ErTransferHeader, data: Array[Byte] = Array.emptyByteArray) extends TransferRpcMessage

case class ErRollSiteHeader(rollSiteSessionId: String,
                            name: String,
                            tag: String,
                            srcRole: String,
                            srcPartyId: String,
                            dstRole: String,
                            dstPartyId: String,
                            dataType: String,
                            options: Map[String, String],
                            totalPartitions: Int,
                            partitionId: Int,
                            totalStreams: Long,
                            totalBatches: Long,
                            streamSeq: Long,
                            batchSeq: Long,
                            stage: String) extends TransferRpcMessage {
  def getRsKey(delim: String = "#", prefix: Array[String] = Array("__rsk")): String = {
    val finalArray = prefix ++ Array(rollSiteSessionId, name, tag, srcRole, srcPartyId, dstRole, dstPartyId)
    String.join(delim, finalArray: _*)
  }

  override def toString: String = {
    super.toString
    s"""<ErRollSiteHeader(rollSiteSessionId=${rollSiteSessionId}, name=${name}, tag=${tag}, srcRole=${srcRole}, srcPartyId=${srcPartyId}, dstRole=${dstRole}, dstPartyId=${dstPartyId}, dataType=${dataType}, options=${options}, totalPartitions=${totalPartitions}, partitionId=${partitionId}, totalStreams=${totalStreams}, totalBatches=${totalBatches}, streamSeq=${streamSeq}, batchSeq=${batchSeq}, stage=${stage}) @ ${Integer.toHexString(hashCode())}>"""
  }
}

object TransferModelPbMessageSerdes {

  // serializers
  implicit class ErTransferHeaderToPbMessage(src: ErTransferHeader) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.TransferHeader = {
      val builder = Transfer.TransferHeader.newBuilder()
        .setId(src.id)
        .setTag(src.tag)
        .setTotalSize(src.totalSize)
        .setStatus(src.status)
        .setExt(ByteString.copyFrom(src.ext))

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErTransferHeader].toBytes()
  }

  implicit class ErTransferBatchToPbMessage(src: ErTransferBatch) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.TransferBatch = {
      val builder = Transfer.TransferBatch.newBuilder()
        .setHeader(src.header.toProto())
        .setBatchSize(src.data.size)

        if (src.data.length > 0) builder.setData(ByteString.copyFrom(src.data))

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErTransferBatch].toBytes()
  }

  implicit class ErRollSiteHeaderToPbMessage(src: ErRollSiteHeader) extends PbMessageSerializer {
    override def toProto[T >: Message](): Transfer.RollSiteHeader = {
      val builder = Transfer.RollSiteHeader.newBuilder()
        .setRollSiteSessionId(src.rollSiteSessionId)
        .setName(src.name)
        .setTag(src.tag)
        .setSrcRole(src.srcRole)
        .setSrcPartyId(src.srcPartyId)
        .setDstRole(src.dstRole)
        .setDstPartyId(src.dstPartyId)
        .setDataType(src.dataType)
        .putAllOptions(src.options.asJava)
        .setTotalPartitions(src.totalPartitions)
        .setPartitionId(src.partitionId)
        .setTotalStreams(src.totalStreams)
        .setTotalBatches(src.totalBatches)
        .setStreamSeq(src.streamSeq)
        .setBatchSeq(src.batchSeq)
        .setStage(src.stage)

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErRollSiteHeader].toBytes()
  }

  // deserializers
  implicit class ErTransferHeaderFromPbMessage(src: Transfer.TransferHeader) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTransferHeader = {
      ErTransferHeader(
        id = src.getId,
        tag = src.getTag,
        totalSize = src.getTotalSize,
        status = src.getStatus,
        ext = src.getExt.toByteArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErTransferHeader =
      Transfer.TransferHeader.parseFrom(bytes).fromProto()
  }

  implicit class ErTransferBatchFromPbMessage(src: Transfer.TransferBatch) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErTransferBatch = {
      ErTransferBatch(header = src.getHeader.fromProto(), data = src.getData.toByteArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErTransferHeader =
      Transfer.TransferHeader.parseFrom(bytes).fromProto()
  }

  implicit class ErRollSiteHeaderFromPbMessage(src: Transfer.RollSiteHeader) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErRollSiteHeader = {
      ErRollSiteHeader(
        rollSiteSessionId = src.getRollSiteSessionId,
        name = src.getName,
        tag = src.getTag,
        srcRole = src.getSrcRole,
        srcPartyId = src.getSrcPartyId,
        dstRole = src.getDstRole,
        dstPartyId = src.getDstPartyId,
        dataType = src.getDataType,
        options = src.getOptionsMap.asScala.toMap,
        totalPartitions = src.getTotalPartitions,
        partitionId = src.getPartitionId,
        totalStreams = src.getTotalStreams,
        totalBatches = src.getTotalBatches,
        streamSeq = src.getStreamSeq,
        batchSeq = src.getBatchSeq,
        stage = src.getStage)
    }

    override def fromBytes(bytes: Array[Byte]): ErRollSiteHeader =
      Transfer.RollSiteHeader.parseFrom(bytes).fromProto()
  }
}

