package com.webank.eggroll.core.deepspeed.job.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes.{ErProcessorFromPbMessage, ErProcessorToPbMessage}
import com.webank.eggroll.core.meta.{Deepspeed, ErProcessor}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class SubmitJobRequest(sessionId: String = StringConstants.EMPTY,
                            name: String = StringConstants.EMPTY,
                            jobType: String = StringConstants.EMPTY,
                            worldSize: Int = 0,
                            commandArguments: Array[String] = Array(),
                            environmentVariables: Map[String, String] = Map(),
                            files: Map[String, Array[Byte]] = Map.empty,
                            zippedFiles: Map[String, Array[Byte]] = Map.empty,
                            options: Map[String, String] = Map(),
                            status: String = StringConstants.EMPTY,
                            processors: Array[ErProcessor] = Array()) {
}

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
      .putAllOptions(src.options.asJava)
      .setStatus(src.status)
      .addAllProcessors(src.processors.toList.map(_.toProto()).asJava)
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
      options = proto.getOptionsMap.asScala.toMap,
      status = proto.getStatus,
      processors = proto.getProcessorsList.asScala.map(_.fromProto()).toArray
    )
  }
}
