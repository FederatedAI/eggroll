package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.meta.Containers

import scala.language.implicitConversions

object ContainerContent {
  implicit def deserialize(byteString: ByteString): ContainerContent = {
    val src = Containers.ContainerContent.parseFrom(byteString)
    ContainerContent(
      containerId = src.getContainerId,
      content = src.getContent.toByteArray,
      compressMethod = src.getCompressMethod
    )
  }

  implicit def toProto(src: ContainerContent): Containers.ContainerContent = {
    val builder = Containers.ContainerContent.newBuilder()
      .setContainerId(src.containerId)
      .setContent(ByteString.copyFrom(src.content))
      .setCompressMethod(src.compressMethod)
    builder.build()
  }

  implicit def serialize(src: ContainerContent): Array[Byte] = {
    toProto(src).toByteArray
  }
}

case class ContainerContent(containerId: Long, content: Array[Byte], compressMethod: String)