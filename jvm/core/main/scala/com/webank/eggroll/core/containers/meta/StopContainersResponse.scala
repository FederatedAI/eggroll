package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants

import scala.language.implicitConversions


case class StopContainersResponse(id: String = StringConstants.EMPTY) {
}

object StopContainersResponse {
  implicit def deserialize(byteString: ByteString): StopContainersResponse = {
    StopContainersResponse()
  }

  implicit def serialize(src: StopContainersResponse): Array[Byte] = {
    Array[Byte]()
  }
}
