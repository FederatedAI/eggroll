package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class DownloadJobRequest(sessionId: String, ranks: Array[Int], compressMethod: String)

object DownloadJobRequest {
  implicit def serialize(src: DownloadJobRequest): Array[Byte] = {
    Deepspeed.DownloadJobRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllRanks(src.ranks.map(_.asInstanceOf[java.lang.Integer]).toSeq.asJava)
      .setCompressMethod(src.compressMethod)
      .build()
      .toByteArray
  }

  implicit def deserialize(byteString: ByteString): DownloadJobRequest = {
    val proto = Deepspeed.DownloadJobRequest.parseFrom(byteString)
    DownloadJobRequest(
      sessionId = proto.getSessionId,
      ranks = proto.getRanksList.asScala.toArray.map(_.toInt),
      compressMethod = proto.getCompressMethod
    )
  }
}
