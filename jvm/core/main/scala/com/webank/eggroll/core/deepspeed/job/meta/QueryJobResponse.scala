package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes.{ErProcessorFromPbMessage, ErProcessorToPbMessage}
import com.webank.eggroll.core.meta.{Deepspeed, ErProcessor}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class QueryJobResponse(
                             sessionId: String = StringConstants.EMPTY,
                             jobType: String = StringConstants.EMPTY,
                             status: String = StringConstants.EMPTY,
                             processors: Array[ErProcessor] = Array[ErProcessor]()
                           )

object QueryJobResponse {
  implicit def serialize(src: QueryJobResponse): Array[Byte] = {
    Deepspeed.QueryJobResponse.newBuilder()
      .setSessionId(src.sessionId)
      .setJobType(src.jobType)
      .setStatus(src.status)
      .addAllProcessors(src.processors.map(_.toProto()).toSeq.asJava)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): QueryJobResponse = {
    val proto = Deepspeed.QueryJobResponse.parseFrom(byteString)
    QueryJobResponse(
      sessionId = proto.getSessionId,
      jobType = proto.getJobType,
      status = proto.getStatus,
      processors = proto.getProcessorsList.asScala.map(_.fromProto()).toArray
    )
  }
}
