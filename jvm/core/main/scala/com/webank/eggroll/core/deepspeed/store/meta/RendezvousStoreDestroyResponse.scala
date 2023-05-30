package com.webank.eggroll.core.deepspeed.store.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class RendezvousStoreDestroyResponse(success: Boolean)

object RendezvousStoreDestroyResponse {
  implicit def serialize(src: RendezvousStoreDestroyResponse): Array[Byte] = {
    val builder = Deepspeed.StoreDestroyResponse.newBuilder()
      .setSuccess(src.success)
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreDestroyResponse = {
    val src = Deepspeed.StoreDestroyResponse.parseFrom(byteString)
    RendezvousStoreDestroyResponse(
      success = src.getSuccess
    )
  }
}
