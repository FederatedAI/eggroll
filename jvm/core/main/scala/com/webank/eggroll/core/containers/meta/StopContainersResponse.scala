package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.language.implicitConversions


case class StopContainersResponse(sessionId: String) {
}

object StopContainersResponse {
  implicit def deserialize(byteString: ByteString): StopContainersResponse = {
    val proto = Containers.StopContainersResponse.parseFrom(byteString)
    StopContainersResponse(proto.getSessionId)
  }

  implicit def serialize(src: StopContainersResponse): Array[Byte] = {
    val builder = Containers.StopContainersResponse.newBuilder()
      .setSessionId(src.sessionId)
    builder.build().toByteArray
  }
}
