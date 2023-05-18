package com.webank.eggroll.core.deepspeed.store.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.language.implicitConversions

case class RendezvousStoreGetRequest(prefix: String, key: K, timeout: FiniteDuration)

object RendezvousStoreGetRequest {
  implicit def serialize(src: RendezvousStoreGetRequest): Array[Byte] = {
    val builder = Deepspeed.StoreGetRequest.newBuilder()
      .setPrefix(src.prefix)
      .setKey(ByteString.copyFrom(src.key.toArray))
      .setTimeout(com.google.protobuf.Duration.newBuilder()
        .setSeconds(src.timeout.toSeconds)
        .setNanos(src.timeout.toNanos.toInt % 1000000000)
        .build())
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): RendezvousStoreGetRequest = {
    val src = Deepspeed.StoreGetRequest.parseFrom(byteString)
    RendezvousStoreGetRequest(
      prefix = src.getPrefix,
      key = src.getKey.toByteArray.toVector,
      timeout = FiniteDuration(src.getTimeout.getSeconds * 1000000000L + src.getTimeout.getNanos, NANOSECONDS)
    )
  }
}
