package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class KillJobRequest(sessionId: String = StringConstants.EMPTY) {
}

object KillJobRequest {
  implicit def serialize(src: KillJobRequest): Array[Byte] = {
    Deepspeed.KillJobRequest.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): KillJobRequest = {
    val proto = Deepspeed.KillJobRequest.parseFrom(byteString)
    KillJobRequest(sessionId = proto.getSessionId)
  }
}
