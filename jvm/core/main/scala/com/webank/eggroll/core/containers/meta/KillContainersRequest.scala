package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErProcessor

import scala.language.implicitConversions

case class KillContainersRequest(
                                  sessionId: String = StringConstants.EMPTY,
                                  processors: Array[ErProcessor] = Array[ErProcessor]()
                                )

object KillContainersRequest {
  implicit def deserialize(byteString: ByteString): KillContainersRequest = {
    KillContainersRequest()
  }

  implicit def serialize(src: KillContainersRequest): Array[Byte] = {
    Array[Byte]()
  }
}
