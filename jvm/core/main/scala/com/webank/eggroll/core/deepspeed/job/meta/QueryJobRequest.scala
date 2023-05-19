package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class QueryJobRequest(sessionId: String = StringConstants.EMPTY) {
}

object QueryJobRequest {
  implicit def serialize(src: QueryJobRequest): Array[Byte] = {
    Deepspeed.QueryJobRequest.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): QueryJobRequest = {
    val proto = Deepspeed.QueryJobRequest.parseFrom(byteString)
    QueryJobRequest(sessionId = proto.getSessionId)
  }
}
