package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class StopJobRequest(sessionId: String = StringConstants.EMPTY) {
}

object StopJobRequest {
  implicit def serialize(src: KillJobRequest): Array[Byte] = {
    Deepspeed.StopJobRequest.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): StopJobRequest = {
    val proto = Deepspeed.StopJobRequest.parseFrom(byteString)
    StopJobRequest(sessionId = proto.getSessionId)
  }
}
