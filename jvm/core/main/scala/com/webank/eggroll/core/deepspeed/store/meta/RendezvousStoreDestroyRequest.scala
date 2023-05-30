package com.webank.eggroll.core.deepspeed.store.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class RendezvousStoreDestroyRequest(prefix: String)

object RendezvousStoreDestroyRequest {
  implicit def serialize(src: RendezvousStoreDestroyRequest): Array[Byte] = {
    val builder = Deepspeed.StoreDestroyRequest.newBuilder()
      .setPrefix(src.prefix)
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreDestroyRequest = {
    val src = Deepspeed.StoreDestroyRequest.parseFrom(byteString)
    RendezvousStoreDestroyRequest(
      prefix = src.getPrefix
    )
  }
}
