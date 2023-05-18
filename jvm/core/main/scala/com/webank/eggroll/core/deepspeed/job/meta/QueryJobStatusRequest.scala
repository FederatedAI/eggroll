package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class QueryJobStatusRequest(sessionId: String)

object QueryJobStatusRequest {
  implicit def serialize(src: QueryJobStatusRequest): Array[Byte] = {
    Deepspeed.QueryJobStatusRequest.newBuilder()
      .setSessionId(src.sessionId)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): QueryJobStatusRequest = {
    val proto = Deepspeed.QueryJobStatusRequest.parseFrom(byteString)
    QueryJobStatusRequest(sessionId = proto.getSessionId)
  }
}
