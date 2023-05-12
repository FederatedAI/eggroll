package com.webank.eggroll.core.deepspeed.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Deepspeed

import scala.language.implicitConversions

case class DeepspeedSubmitJobResponse()

object DeepspeedSubmitJobResponse {
  implicit def serialize(src: DeepspeedSubmitJobResponse): Array[Byte] = {
    val builder = Deepspeed.SubmitJobResponse.newBuilder()
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): DeepspeedSubmitJobResponse = {
    val proto = Deepspeed.SubmitJobResponse.parseFrom(byteString)
    DeepspeedSubmitJobResponse()
  }
}
