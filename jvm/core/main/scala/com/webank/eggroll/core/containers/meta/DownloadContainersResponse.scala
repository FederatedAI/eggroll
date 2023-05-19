package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions


case class DownloadContainersResponse(sessionId: String, containerContents: Array[ContainerContent])

object DownloadContainersResponse {
  implicit def deserialize(byteString: ByteString): DownloadContainersResponse = {
    val src = Containers.DownloadContainersResponse.parseFrom(byteString)
    DownloadContainersResponse(
      sessionId = src.getSessionId,
      containerContents = src.getContainerContentList.asScala.map(v =>
        ContainerContent(v.getContainerId, v.getContent.toByteArray, v.getCompressMethod)).toArray
    )
  }

  implicit def serialize(src: DownloadContainersResponse): Array[Byte] = {
    val builder = Containers.DownloadContainersResponse.newBuilder()
      .setSessionId(src.sessionId)
      .addAllContainerContent(src.containerContents.map(ContainerContent.toProto).toList.asJava)
    builder.build().toByteArray
  }
}
