package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants

import scala.language.implicitConversions

case class StopContainersRequest(id: String = StringConstants.EMPTY) {
}

object StopContainersRequest {
  implicit def deserialize(byteString: ByteString): StopContainersRequest = {
    StopContainersRequest()
  }

  implicit def serialize(src: StopContainersRequest): Array[Byte] = {
    Array[Byte]()
  }
}
