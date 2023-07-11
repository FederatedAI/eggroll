package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.containers.meta.ContentType
import com.webank.eggroll.core.meta.Deepspeed
import com.webank.eggroll.core.meta.DeepspeedDownload.PrepareDownloadRequest

import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.language.implicitConversions

case class PrepareJobDownloadRequest(sessionId: String, ranks: Array[Int],
                                     compressMethod: String,
                                     compressLevel: Int = 1,
                                     contentType: ContentType.ContentType)


object PrepareJobDownloadRequest {
  implicit def serialize(src: PrepareJobDownloadRequest): Array[Byte] = {
    PrepareDownloadRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllRanks(src.ranks.map(_.asInstanceOf[java.lang.Integer]).toSeq.asJava)
      .setCompressMethod(src.compressMethod)
      .setCompressLevel(src.compressLevel)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): PrepareJobDownloadRequest = {
    val proto = PrepareDownloadRequest.parseFrom(byteString)
    PrepareJobDownloadRequest(
      sessionId = proto.getSessionId,
      ranks = proto.getRanksList.asScala.toArray.map(_.toInt),
      compressMethod = proto.getCompressMethod,
      compressLevel = proto.getCompressLevel,
      contentType = proto.getContentType
    )
  }
}