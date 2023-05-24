package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class StopContainersRequest(sessionId: String, containers: Array[Long]) {
}

object StopContainersRequest {
  implicit def deserialize(byteString: ByteString): StopContainersRequest = {
    val proto = Containers.StopContainersRequest.parseFrom(byteString)
    StopContainersRequest(proto.getSessionId, proto.getContainerIdsList.asScala.map(_.toLong)(collection.breakOut))
  }

  implicit def serialize(src: StopContainersRequest): Array[Byte] = {
    Containers.StopContainersRequest.newBuilder()
      .setSessionId(src.sessionId)
      .addAllContainerIds(src.containers.map(_.asInstanceOf[java.lang.Long]).toSeq.asJava)
      .build().toByteArray
  }
}
