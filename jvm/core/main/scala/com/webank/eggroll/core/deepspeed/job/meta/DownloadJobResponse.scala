package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.containers.meta.ContainerContent
import com.webank.eggroll.core.meta.Deepspeed

import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.language.implicitConversions

case class DownloadJobResponse(sessionId: String, containerContents: Array[ContainerContent])

object DownloadJobResponse {

  implicit def deserialize(byteString: ByteString): DownloadJobResponse = {
    val proto = Deepspeed.DownloadJobResponse.parseFrom(byteString)
    DownloadJobResponse(
      sessionId = proto.getSessionId,
      containerContents = proto.getContainerContentList.asScala.map(v =>
        ContainerContent(v.getContainerId, v.getContent.toByteArray, v.getCompressMethod)).toArray)
  }

  implicit def serialize(src: DownloadJobResponse): Array[Byte] = {
    Deepspeed.DownloadJobResponse.newBuilder()
      .setSessionId(src.sessionId)
      .addAllContainerContent(src.containerContents.map(ContainerContent.toProto).toList.asJava)
      .build()
      .toByteArray
  }
}