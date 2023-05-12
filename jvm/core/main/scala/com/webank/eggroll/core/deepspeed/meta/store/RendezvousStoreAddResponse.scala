package com.webank.eggroll.core.deepspeed.meta.store

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class RendezvousStoreAddResponse(amount: Long)

object RendezvousStoreAddResponse {
  implicit def serialize(src: RendezvousStoreAddResponse): Array[Byte] = {
    val builder = Deepspeed.StoreAddResponse.newBuilder()
      .setAmount(src.amount)
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreAddResponse = {
    val src = Deepspeed.StoreAddResponse.parseFrom(byteString)
    RendezvousStoreAddResponse(
      amount = src.getAmount
    )
  }
}