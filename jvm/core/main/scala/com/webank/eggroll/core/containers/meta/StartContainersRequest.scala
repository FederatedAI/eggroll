package com.webank.eggroll.core.containers.meta

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.containers.JobProcessorTypes
import com.webank.eggroll.core.meta.Containers

import scala.collection.JavaConverters._
import scala.language.implicitConversions


case class DeepspeedContainerConfig(
                                     cudaVisibleDevices: Array[Int],
                                     worldSize: Int,
                                     crossRank: Int,
                                     crossSize: Int,
                                     localSize: Int,
                                     localRank: Int,
                                     rank: Int,
                                     storePrefix: String,
                                     storeHost: Option[String] = None,
                                     storePort: Option[Int] = None,
                                     backend: Option[String] = None
                                   )

object DeepspeedContainerConfig {
  implicit def deserialize(byteString: ByteString): DeepspeedContainerConfig = {
    val src = Containers.DeepspeedContainerConfig.parseFrom(byteString)
    DeepspeedContainerConfig(
      cudaVisibleDevices = src.getCudaVisibleDevicesList.asScala.toArray.map(_.toInt),
      worldSize = src.getWorldSize,
      crossRank = src.getCrossRank,
      crossSize = src.getCrossSize,
      localSize = src.getLocalSize,
      localRank = src.getLocalRank,
      rank = src.getRank,
      storeHost = Option(src.getStoreHost).filter(_.nonEmpty),
      storePort = Option(src.getStorePort).filter(_ > 0),
      backend = Option(src.getBackend).filter(_.nonEmpty),
      storePrefix = src.getStorePrefix
    )
  }

  implicit def serialize(src: DeepspeedContainerConfig): Array[Byte] = {
    val builder = Containers.DeepspeedContainerConfig.newBuilder()
      .addAllCudaVisibleDevices(src.cudaVisibleDevices.toList.map(_.asInstanceOf[java.lang.Integer]).asJava)
      .setWorldSize(src.worldSize)
      .setCrossRank(src.crossRank)
      .setCrossSize(src.crossSize)
      .setLocalSize(src.localSize)
      .setLocalRank(src.localRank)
      .setRank(src.rank)
      .setStoreHost(src.storeHost.getOrElse(StringConstants.EMPTY))
      .setStorePort(src.storePort.getOrElse(-1))
      .setBackend(src.backend.getOrElse(StringConstants.EMPTY))
      .setStorePrefix(src.storePrefix)
    builder.build().toByteArray
  }
}

case class StartDeepspeedContainerRequest(sessionId: String = StringConstants.EMPTY,
                                          name: String = StringConstants.EMPTY,
                                          commandArguments: Array[String] = Array(),
                                          environmentVariables: Map[String, String] = Map(),
                                          files: Map[String, Array[Byte]] = Map.empty,
                                          zippedFiles: Map[String, Array[Byte]] = Map.empty,
                                          options: Map[String, String] = Map(),
                                          deepspeedConfigs: Map[Long, DeepspeedContainerConfig]) {
}

object StartDeepspeedContainerRequest {
  implicit def fromStartContainersRequest(src: StartContainersRequest): StartDeepspeedContainerRequest = {
    StartDeepspeedContainerRequest(
      sessionId = src.sessionId,
      name = src.name,
      commandArguments = src.commandArguments,
      environmentVariables = src.environmentVariables,
      files = src.files,
      zippedFiles = src.zippedFiles,
      options = src.options,
      deepspeedConfigs = src.typedExtraConfigs.mapValues { v =>
        DeepspeedContainerConfig.deserialize(ByteString.copyFrom(v))
      })
  }

  implicit def toStartContainersRequest(src: StartDeepspeedContainerRequest): StartContainersRequest = {
    StartContainersRequest(
      sessionId = src.sessionId,
      name = src.name,
      jobType = Some(JobProcessorTypes.DeepSpeed),
      commandArguments = src.commandArguments,
      environmentVariables = src.environmentVariables,
      files = src.files,
      zippedFiles = src.zippedFiles,
      options = src.options,
      typedExtraConfigs = src.deepspeedConfigs.mapValues(DeepspeedContainerConfig.serialize)
    )
  }
}

case class StartContainersRequest(sessionId: String = StringConstants.EMPTY,
                                  name: String = StringConstants.EMPTY,
                                  jobType: Option[JobProcessorTypes.Value] = None,
                                  commandArguments: Array[String] = Array(),
                                  environmentVariables: Map[String, String] = Map(),
                                  files: Map[String, Array[Byte]] = Map.empty,
                                  zippedFiles: Map[String, Array[Byte]] = Map.empty,
                                  typedExtraConfigs: Map[Long, Array[Byte]] = Map(),
                                  options: Map[String, String] = Map()) {
}

object StartContainersRequest {
  implicit def deserialize(byteString: ByteString): StartContainersRequest = {
    val src = Containers.StartContainersRequest.parseFrom(byteString)
    StartContainersRequest(
      sessionId = src.getSessionId,
      name = src.getName,
      jobType = JobProcessorTypes.fromString(src.getJobType),
      commandArguments = src.getCommandArgumentsList.asScala.toArray,
      environmentVariables = src.getEnvironmentVariablesMap.asScala.toMap,
      files = src.getFilesMap.asScala.toMap.mapValues(_.toByteArray),
      zippedFiles = src.getZippedFilesMap.asScala.toMap.mapValues(_.toByteArray),
      typedExtraConfigs = src.getTypedExtraConfigsMap.asScala.map(kv => (kv._1.toLong, kv._2.toByteArray)).toMap,
      options = src.getOptionsMap.asScala.toMap
    )
  }

  implicit def serialize(src: StartContainersRequest): Array[Byte] = {
    val builder = Containers.StartContainersRequest.newBuilder()
      .setSessionId(src.sessionId)
      .setName(src.name)
      .addAllCommandArguments(src.commandArguments.toList.asJava)
      .putAllEnvironmentVariables(src.environmentVariables.asJava)
      .putAllFiles(src.files.mapValues(ByteString.copyFrom).asJava)
      .putAllZippedFiles(src.zippedFiles.mapValues(ByteString.copyFrom).asJava)
      .putAllTypedExtraConfigs(src.typedExtraConfigs.map { case (k, v) =>
        (k.asInstanceOf[java.lang.Long], ByteString.copyFrom(v))
      }.asJava)
      .putAllOptions(src.options.asJava)
    src.jobType match {
      case Some(jobType) => builder.setJobType(jobType.toString)
      case None => builder.setJobType(StringConstants.EMPTY)
    }
    builder.build().toByteArray
  }
}