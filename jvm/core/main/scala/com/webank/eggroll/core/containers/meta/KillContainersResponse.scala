package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants

import scala.language.implicitConversions


case class KillContainersResponse(id: String = StringConstants.EMPTY) {
}

object KillContainersResponse {
  implicit def deserialize(byteString: ByteString): KillContainersResponse = {
    KillContainersResponse()
  }

  implicit def serialize(src: KillContainersResponse): Array[Byte] = {
    Array[Byte]()
  }
}
