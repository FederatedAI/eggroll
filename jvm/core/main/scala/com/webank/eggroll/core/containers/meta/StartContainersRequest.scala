package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes.{ErProcessorFromPbMessage, ErProcessorToPbMessage}
import com.webank.eggroll.core.meta.{Containers, ErProcessor}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class StartContainersRequest(id: String = StringConstants.EMPTY,
                                  name: String = StringConstants.EMPTY,
                                  jobType: String = StringConstants.EMPTY,
                                  worldSize: Int = 0,
                                  commandArguments: Array[String] = Array(),
                                  environmentVariables: Map[String, String] = Map(),
                                  files: Map[String, Array[Byte]] = Map.empty,
                                  zippedFiles: Map[String, Array[Byte]] = Map.empty,
                                  options: Map[String, String] = Map(),
                                  processors: Array[ErProcessor] = Array()) {
}

object StartContainersRequest {
  implicit def deserialize(byteString: ByteString): StartContainersRequest = {
    val src = Containers.StartContainersRequest.parseFrom(byteString)
    StartContainersRequest(
      id = src.getSessionId,
      name = src.getName,
      jobType = src.getJobType,
      worldSize = src.getWorldSize,
      commandArguments = src.getCommandArgumentsList.asScala.toArray,
      environmentVariables = src.getEnvironmentVariablesMap.asScala.toMap,
      files = src.getFilesMap.asScala.toMap.mapValues(_.toByteArray),
      zippedFiles = src.getZippedFilesMap.asScala.toMap.mapValues(_.toByteArray),
      options = src.getOptionsMap.asScala.toMap,
      processors = src.getProcessorsList.asScala.map(_.fromProto()).toArray
    )
  }

  implicit def serialize(src: StartContainersRequest): Array[Byte] = {
    val builder = Containers.StartContainersRequest.newBuilder()
      .setSessionId(src.id)
      .setName(src.name)
      .setJobType(src.jobType)
      .setWorldSize(src.worldSize)
      .addAllCommandArguments(src.commandArguments.toList.asJava)
      .putAllEnvironmentVariables(src.environmentVariables.asJava)
      .putAllFiles(src.files.mapValues(ByteString.copyFrom).asJava)
      .putAllZippedFiles(src.zippedFiles.mapValues(ByteString.copyFrom).asJava)
      .putAllOptions(src.options.asJava)
      .addAllProcessors(src.processors.toList.map(_.toProto()).asJava)
    builder.build().toByteArray
  }
}