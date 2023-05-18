package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes.{ErProcessorFromPbMessage, ErProcessorToPbMessage}
import com.webank.eggroll.core.meta.{Deepspeed, ErProcessor}

import scala.language.implicitConversions
import scala.collection.JavaConverters._

case class SubmitJobResponse(sessionId: String, processors: Array[ErProcessor])

object SubmitJobResponse {
  implicit def serialize(src: SubmitJobResponse): Array[Byte] = {
    Deepspeed.SubmitJobResponse.newBuilder()
      .setSessionId(src.sessionId)
      .addAllProcessors(src.processors.toList.map(_.toProto()).asJava)
      .build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): SubmitJobResponse = {
    val proto = Deepspeed.SubmitJobResponse.parseFrom(byteString)
    SubmitJobResponse(sessionId = proto.getSessionId,
      processors = proto.getProcessorsList.asScala.map(_.fromProto()).toArray)
  }
}
