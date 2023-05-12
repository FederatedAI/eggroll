package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.Containers

import scala.language.implicitConversions


case class StartContainersResponse(id: String = StringConstants.EMPTY) {
}

object StartContainersResponse {
  implicit def deserialize(byteString: ByteString): StartContainersResponse = {
    val src = Containers.StartContainersResponse.parseFrom(byteString)
    StartContainersResponse(
      id = src.getSessionId
    )
  }

  implicit def serialize(src: StartContainersResponse): Array[Byte] = {
    val builder = Containers.StartContainersResponse.newBuilder()
      .setSessionId(src.id)
    builder.build().toByteArray
  }
}
