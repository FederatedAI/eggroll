package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class StopJobResponse(sessionId: String)

object StopJobResponse {
  implicit def serialize(src: StopJobResponse): Array[Byte] = {
    Deepspeed.StopJobResponse.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): StopJobResponse = {
    val proto = Deepspeed.StopJobResponse.parseFrom(byteString)
    StopJobResponse(sessionId = proto.getSessionId)
  }
}
