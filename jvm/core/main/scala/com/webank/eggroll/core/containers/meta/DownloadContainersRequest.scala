package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class DownloadContainersRequest(
                                      sessionId: String,
                                      containerIds: Array[Long],
                                      compressMethod: String = "zip"
                                    )

object DownloadContainersRequest {
  implicit def deserialize(byteString: ByteString): DownloadContainersRequest = {
    val src = Containers.DownloadContainersRequest.parseFrom(byteString)
    DownloadContainersRequest(
      sessionId = src.getSessionId,
      containerIds = src.getContainerIdsList.asScala.map(_.toLong).toArray,
      compressMethod = src.getCompressMethod
    )
  }

  implicit def serialize(src: DownloadContainersRequest): Array[Byte] = {
    val builder = Containers.DownloadContainersRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllContainerIds(src.containerIds.map(_.asInstanceOf[java.lang.Long]).toList.asJava)
      .setCompressMethod(src.compressMethod)
    builder.build().toByteArray
  }
}
