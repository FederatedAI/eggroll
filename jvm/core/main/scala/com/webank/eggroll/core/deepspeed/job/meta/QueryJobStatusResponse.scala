package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class QueryJobStatusResponse(sessionId: String, status: String)

object QueryJobStatusResponse {
  implicit def serialize(src: QueryJobStatusResponse): Array[Byte] = {
    Deepspeed.QueryJobStatusResponse.newBuilder()
      .setSessionId(src.sessionId)
      .setStatus(src.status)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): QueryJobStatusResponse = {
    val proto = Deepspeed.QueryJobStatusResponse.parseFrom(byteString)
    QueryJobStatusResponse(
      sessionId = proto.getSessionId,
      status = proto.getStatus
    )
  }
}
