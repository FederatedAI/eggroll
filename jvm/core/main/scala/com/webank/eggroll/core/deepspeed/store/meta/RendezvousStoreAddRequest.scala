package com.webank.eggroll.core.deepspeed.store.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class RendezvousStoreAddRequest(prefix: String, key: K, amount: Long)

object RendezvousStoreAddRequest {
  implicit def serialize(src: RendezvousStoreAddRequest): Array[Byte] = {
    val builder = Deepspeed.StoreAddRequest.newBuilder()
      .setPrefix(src.prefix)
      .setKey(ByteString.copyFrom(src.key.toArray))
      .setAmount(src.amount)
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreAddRequest = {
    val src = Deepspeed.StoreAddRequest.parseFrom(byteString)
    RendezvousStoreAddRequest(
      prefix = src.getPrefix,
      key = src.getKey.toByteArray.toVector,
      amount = src.getAmount
    )
  }
}
