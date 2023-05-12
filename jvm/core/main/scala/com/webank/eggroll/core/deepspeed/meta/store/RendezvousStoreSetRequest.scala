package com.webank.eggroll.core.deepspeed.meta.store

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class RendezvousStoreSetRequest(prefix: String, key: K, value: V)

object RendezvousStoreSetRequest {
  implicit def serialize(src: RendezvousStoreSetRequest): Array[Byte] = {
    val builder = Deepspeed.StoreSetRequest.newBuilder()
      .setPrefix(src.prefix)
      .setKey(ByteString.copyFrom(src.key.toArray))
      .setValue(ByteString.copyFrom(src.value.toArray))
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreSetRequest = {
    val proto = Deepspeed.StoreSetRequest.parseFrom(byteString)
    RendezvousStoreSetRequest(
      prefix = proto.getPrefix,
      key = proto.getKey.toByteArray.toVector,
      value = proto.getValue.toByteArray.toVector
    )
  }
}