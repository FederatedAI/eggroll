package com.webank.eggroll.core.deepspeed.store.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions


case class RendezvousStoreSetResponse()

object RendezvousStoreSetResponse {
  implicit def serialize(src: RendezvousStoreSetResponse): Array[Byte] = {
    val builder = Deepspeed.StoreSetResponse.newBuilder()
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreSetResponse = {
    val proto = Deepspeed.StoreSetResponse.parseFrom(byteString)
    RendezvousStoreSetResponse()
  }
}
