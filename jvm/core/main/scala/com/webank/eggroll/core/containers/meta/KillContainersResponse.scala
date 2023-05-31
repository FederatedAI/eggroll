package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.language.implicitConversions


case class KillContainersResponse(sessionId: String) {
}

object KillContainersResponse {
  implicit def deserialize(byteString: ByteString): KillContainersResponse = {
    val proto = Containers.KillContainersResponse.parseFrom(byteString)
    KillContainersResponse(proto.getSessionId)
  }

  implicit def serialize(src: KillContainersResponse): Array[Byte] = {
    val builder = Containers.KillContainersResponse.newBuilder()
      .setSessionId(src.sessionId)
    builder.build().toByteArray
  }
}
