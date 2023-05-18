package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class KillJobResponse(sessionId: String)

object KillJobResponse {
  implicit def serialize(src: KillJobResponse): Array[Byte] = {
    Deepspeed.KillJobResponse.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): KillJobResponse = {
    val proto = Deepspeed.KillJobResponse.parseFrom(byteString)
    KillJobResponse(proto.getSessionId)
  }
}
