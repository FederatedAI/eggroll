package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class DownloadContainersRequest(
                                      sessionId: String,
                                      ranks: Array[Int],
                                      compressMethod: String = "zip",
                                      compressLevel: Int = 1,
                                      contentType: ContentType.ContentType
                                    )


object DownloadContainersRequest {
  implicit def deserialize(byteString: ByteString): DownloadContainersRequest = {
    val src = Containers.DownloadContainersRequest.parseFrom(byteString)
    DownloadContainersRequest(
      sessionId = src.getSessionId,
      ranks = src.getRanksList.asScala.map(_.toInt).toArray,
      compressMethod = src.getCompressMethod,
      compressLevel = src.getCompressLevel,
      contentType = src.getContentType
    )
  }

  implicit def serialize(src: DownloadContainersRequest): Array[Byte] = {
    val builder = Containers.DownloadContainersRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllRanks(src.ranks.map(_.asInstanceOf[java.lang.Integer]).toList.asJava)
      .setCompressMethod(src.compressMethod)
      .setCompressLevel(src.compressLevel)
      .setContentType(src.contentType)
    builder.build().toByteArray
  }
}
