package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class KillContainersRequest(sessionId: String, containers: Array[Long])

object KillContainersRequest {
  implicit def deserialize(byteString: ByteString): KillContainersRequest = {
    val proto = Containers.KillContainersRequest.parseFrom(byteString)
    KillContainersRequest(proto.getSessionId, proto.getContainerIdsList.asScala.map(_.toLong)(collection.breakOut))
  }

  implicit def serialize(src: KillContainersRequest): Array[Byte] = {
    Containers.KillContainersRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllContainerIds(src.containers.map(_.asInstanceOf[java.lang.Long]).toSeq.asJava)
      .build().toByteArray
  }
}
