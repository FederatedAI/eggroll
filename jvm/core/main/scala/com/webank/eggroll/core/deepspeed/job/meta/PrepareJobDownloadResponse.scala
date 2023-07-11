package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.containers.meta.ContainerContent
import com.webank.eggroll.core.meta.Deepspeed
import com.webank.eggroll.core.meta.DeepspeedDownload.{PrepareDownloadRequest, PrepareDownloadResponse}

import scala.language.implicitConversions

case class PrepareJobDownloadResponse(sessionId: String, content :String)

object PrepareJobDownloadResponse {

  implicit def deserialize(byteString: ByteString): PrepareJobDownloadResponse = {
    val proto = PrepareDownloadResponse.parseFrom(byteString)
    PrepareJobDownloadResponse(
      sessionId = proto.getSessionId,
      content = proto.getContent)
  }

  implicit def serialize(src: PrepareJobDownloadResponse): Array[Byte] = {
    PrepareDownloadResponse.newBuilder()
      .setSessionId(src.sessionId)
      .setContent(src.content).build()
      .toByteArray
  }
}
