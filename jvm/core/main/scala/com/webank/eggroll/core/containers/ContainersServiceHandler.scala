package com.webank.eggroll.core.containers

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ProcessorStatus}
import com.webank.eggroll.core.containers.ContainersServiceHandler.{CompressMethod, zip}
import com.webank.eggroll.core.containers.container.{ContainersManager, DeepSpeedContainer, WarpedDeepspeedContainerConfig}
import com.webank.eggroll.core.containers.meta._
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.resourcemanager.NodeManagerMeta
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.concurrent.ExecutionContext
import scala.reflect.io.Path


class ContainersServiceHandler(implicit ec: ExecutionContext,
                               providedContainersDataDir: Option[Path] = None) extends Logging {

  // containersDataDir is essential for containers to work
  // we assume that all basic data related to containers are stored in this dir, including:
  // 1. container `binaries`
  // 2. container logs
  // 3. container generated data and models
  // 4. others that are not listed here (TODO: add them here)
  // we expose this dir as parameter of this class for testing purpose,
  // but in production, we should always use the default value from config
  private lazy val containersDataDir: Path = {
    providedContainersDataDir.getOrElse {
      val pathStr = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_CONTAINERS_DATA_DIR)

      if (pathStr == null || pathStr.isEmpty) {
        throw new IllegalArgumentException("container data dir not set")
      }
      val path = Path(pathStr)
      if (!path.exists) {
        path.createDirectory()
      }
      path
    }
  }


  var client = new ClusterManagerClient()
  private val containersManager = ContainersManager.builder()
    .withStartedCallback(container => {
      val pid = container.getPid()
      val status = if (pid > 0) ProcessorStatus.RUNNING else ProcessorStatus.ERROR
      client.heartbeat(ErProcessor(id = container.getProcessorId(), pid = pid,
        serverNodeId = NodeManagerMeta.serverNodeId, status = status))
      logInfo(s"(${container.getProcessorId()})container started: $container ${container.getPid()}, $status")
    })
    .withSuccessCallback(container => {
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.FINISHED))
      logInfo(s"${container.getProcessorId()})container success: $container ${container.getPid()}")
    })
    .withFailedCallback(container => {
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.ERROR))
      logInfo(s"(${container.getProcessorId()})container failed: $container ${container.getPid()}")
    })
    .withExceptionCallback((container, e) => {
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.KILLED))
      logInfo(s"(${container.getProcessorId()})container exception: $container ${container.getPid()}, $e")
    })
    .build

  def startJobContainers(startContainersRequest: StartContainersRequest): StartContainersResponse = {
    startContainersRequest.jobType match {
      case Some(JobProcessorTypes.DeepSpeed) => startDeepspeedContainers(startContainersRequest)
      case _ => throw new IllegalArgumentException(s"unsupported job type: ${startContainersRequest.jobType}")
    }
  }

  private def startDeepspeedContainers(startDeepspeedContainerRequest: StartDeepspeedContainerRequest): StartContainersResponse = {
    val sessionId = startDeepspeedContainerRequest.sessionId
    logInfo(s"(sessionId=$sessionId) starting deepspeed containers")
    startDeepspeedContainerRequest.deepspeedConfigs.par.foreach { case (containerId, deepspeedConfig) =>
      val container = new DeepSpeedContainer(
        sessionId = sessionId,
        processorId = containerId,
        deepspeedContainerConfig = new WarpedDeepspeedContainerConfig(deepspeedConfig),
        containerWorkspace = getContainerWorkspace(containerId),
        commandArguments = startDeepspeedContainerRequest.commandArguments,
        environmentVariables = startDeepspeedContainerRequest.environmentVariables,
        files = startDeepspeedContainerRequest.files,
        zippedFiles = startDeepspeedContainerRequest.zippedFiles,
        options = startDeepspeedContainerRequest.options
      )
      containersManager.addContainer(containerId, container)
      containersManager.startContainer(containerId)
      logInfo(s"(sessionId=$sessionId) deepspeed container started: $containerId")
    }
    logInfo(s"(sessionId=$sessionId) deepspeed containers started")
    StartContainersResponse(sessionId)
  }


  def stopJobContainers(stopContainersRequest: StopContainersRequest): StopContainersResponse = {
    val sessionId = stopContainersRequest.sessionId
    logInfo(s"(sessionId=${stopContainersRequest.sessionId})stopping containers")
    stopContainersRequest.containers.foreach { containerId =>
      containersManager.stopContainer(containerId)
    }
    StopContainersResponse(sessionId)
  }

  def killJobContainers(killContainersRequest: KillContainersRequest): KillContainersResponse = {
    val sessionId = killContainersRequest.sessionId
    logInfo(s"(sessionId=$sessionId)killing containers")
    killContainersRequest.containers.foreach { containerId =>
      containersManager.killContainer(containerId)
    }
    KillContainersResponse(sessionId)
  }

  def downloadContainers(downloadContainersRequest: DownloadContainersRequest): DownloadContainersResponse = {
    val sessionId = downloadContainersRequest.sessionId
    val containerContentType = downloadContainersRequest.contentType
    val containerIds = downloadContainersRequest.containerIds
    logInfo(s"(sessionId=$sessionId)downloading containers: ${containerIds.mkString(",")}")

    val contents = containerIds.map { containerId =>
      val targetDir = containerContentType match {
        case ContentType.ALL => getContainerWorkspace(containerId)
        case ContentType.MODELS => getContainerModelsDir(containerId)
        case ContentType.LOGS => getContainerLogsDir(containerId)
        case _ => throw new IllegalArgumentException(s"unsupported container content type: $containerContentType")
      }
      downloadContainersRequest.compressMethod match {
        case CompressMethod.ZIP =>
          if (targetDir.exists)
            ContainerContent(containerId, zip(targetDir, downloadContainersRequest.compressLevel), CompressMethod.ZIP)
          else
            ContainerContent(containerId, Array[Byte](), CompressMethod.ZIP)
        case _ =>
          throw new IllegalArgumentException(s"compress method not supported: ${downloadContainersRequest.compressMethod}")
      }
    }
    DownloadContainersResponse(sessionId = downloadContainersRequest.sessionId, containerContents = contents)
  }

  private def getContainerWorkspace(containerId: Long): Path = {
    containersDataDir / containerId.toString
  }

  private def getContainerModelsDir(containerId: Long): Path = {
    getContainerWorkspace(containerId) / MODELS
  }

  private def getContainerLogsDir(containerId: Long): Path = {
    getContainerWorkspace(containerId) / LOGS
  }
}

object ContainersServiceHandler extends Logging {

  object CompressMethod {
    val ZIP = "zip"
  }

  def zip(path: Path, level: Int): Array[Byte] = {
    logInfo(s"zipping path: $path")
    val byteStream = new ByteArrayOutputStream()
    val zipOutput = new ZipOutputStream(byteStream)
    zipOutput.setLevel(level)
    try {
      path.walk.foreach(subPath => {
        if (Files.isRegularFile(subPath.jfile.toPath)) {
          val name = path.relativize(subPath).toString
          zipOutput.putNextEntry(new ZipEntry(name))
          val in = new FileInputStream(subPath.jfile)
          val buffer = new Array[Byte](1024)
          var bytesRead = in.read(buffer)
          while (bytesRead != -1) {
            zipOutput.write(buffer, 0, bytesRead)
            bytesRead = in.read(buffer)
          }
          in.close()
          zipOutput.closeEntry()
        }
      })
    } finally {
      zipOutput.close()
    }
    logInfo(s"zipped path: $path")
    byteStream.toByteArray
  }
}