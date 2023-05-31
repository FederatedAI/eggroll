package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.Deepspeed

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class ResourceOptions(
                            timeoutSeconds: Int = 0,
                            resourceExhaustedStrategy: String
                          )

case class SubmitJobRequest(sessionId: String = StringConstants.EMPTY,
                            name: String = StringConstants.EMPTY,
                            jobType: String = StringConstants.EMPTY,
                            worldSize: Int = 0,
                            commandArguments: Array[String] = Array(),
                            environmentVariables: Map[String, String] = Map(),
                            files: Map[String, Array[Byte]] = Map.empty,
                            zippedFiles: Map[String, Array[Byte]] = Map.empty,
                            resourceOptions: ResourceOptions,
                            options: Map[String, String] = Map()
                           )

object SubmitJobRequest {
  implicit def serialize(src: SubmitJobRequest): Array[Byte] = {
    val builder = Deepspeed.SubmitJobRequest.newBuilder()
      .setSessionId(src.sessionId)
      .setName(src.name)
      .setJobType(src.jobType)
      .setWorldSize(src.worldSize)
      .addAllCommandArguments(src.commandArguments.toList.asJava)
      .putAllEnvironmentVariables(src.environmentVariables.asJava)
      .putAllFiles(src.files.mapValues(ByteString.copyFrom).asJava)
      .putAllZippedFiles(src.zippedFiles.mapValues(ByteString.copyFrom).asJava)
      .setResourceOptions(
        Deepspeed.ResourceOptions.newBuilder()
          .setTimeoutSeconds(src.resourceOptions.timeoutSeconds)
          .setResourceExhaustedStrategy(src.resourceOptions.resourceExhaustedStrategy)
          .build())
      .putAllOptions(src.options.asJava)
    builder.build().toByteArray
  }

  implicit def deserialize(byteString: ByteString): SubmitJobRequest = {
    val proto = Deepspeed.SubmitJobRequest.parseFrom(byteString)
    SubmitJobRequest(
      sessionId = proto.getSessionId,
      name = proto.getName,
      jobType = proto.getJobType,
      worldSize = proto.getWorldSize,
      commandArguments = proto.getCommandArgumentsList.asScala.toArray,
      environmentVariables = proto.getEnvironmentVariablesMap.asScala.toMap,
      files = proto.getFilesMap.asScala.mapValues(_.toByteArray).toMap,
      zippedFiles = proto.getZippedFilesMap.asScala.mapValues(_.toByteArray).toMap,
      resourceOptions = ResourceOptions(
        timeoutSeconds = proto.getResourceOptions.getTimeoutSeconds,
        resourceExhaustedStrategy = proto.getResourceOptions.getResourceExhaustedStrategy
      ),
      options = proto.getOptionsMap.asScala.toMap
    )
  }
}
