package com.webank.eggroll.core.containers

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ProcessorStatus}
import com.webank.eggroll.core.containers.ContainersServiceHandler.{CompressMethod, zip}
import com.webank.eggroll.core.containers.container.{ContainersManager, DeepSpeedContainer, WrapedDeepspeedContainerConfig}
import com.webank.eggroll.core.containers.meta._
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.resourcemanager.NodeManagerMeta
import com.webank.eggroll.core.session.StaticErConf

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.concurrent.ExecutionContext
import scala.reflect.io.Path


class ContainersServiceHandler(implicit ec: ExecutionContext,
                               providedContainersDataDir: Option[Path] = None) {

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
      var pathStr = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_CONTAINERS_DATA_DIR)

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
    // TODO: status callbacks here
    .withStartedCallback((container) => {
      //      object ProcessorStatus {
      //        val NEW = "NEW"
      //        val RUNNING = "RUNNING"
      //        val STOPPED = "STOPPED"
      //        val KILLED = "KILLED"
      //        val ERROR = "ERROR"
      //      }

      println(s"container started: ${container} ${container.getPid()} ")
      var pid = container.getPid()
      var status = if (pid > 0) ProcessorStatus.RUNNING else ProcessorStatus.ERROR
      client.heartbeat(ErProcessor(id = container.getProcessorId(), pid = pid,
        serverNodeId = NodeManagerMeta.serverNodeId, status = status));

    })
    .withSuccessCallback((container) => {
      println(s"container success: ${container}")
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.STOPPED));

    })
    .withFailedCallback((container) => {
      println(s"container failed: ${container}")
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.ERROR));
    })
    .withExceptionCallback((container, e) => {
      println(s"container exception: ${container}, ${e}")
      client.heartbeat(ErProcessor(id = container.getProcessorId(), serverNodeId = NodeManagerMeta.serverNodeId, status = ProcessorStatus.KILLED));
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
    startDeepspeedContainerRequest.deepspeedConfigs.par.foreach { case (containerId, deepspeedConfig) =>
      val container = {
        new DeepSpeedContainer(
          sessionId = sessionId,
          processorId = containerId,
          deepspeedContainerConfig = new WrapedDeepspeedContainerConfig(deepspeedConfig),
          containerWorkspace = getContainerWorkspace(containerId),
          commandArguments = startDeepspeedContainerRequest.commandArguments,
          environmentVariables = startDeepspeedContainerRequest.environmentVariables,
          files = startDeepspeedContainerRequest.files,
          zippedFiles = startDeepspeedContainerRequest.zippedFiles,
          options = startDeepspeedContainerRequest.options
        )
      }
      containersManager.addContainer(containerId, container)
      containersManager.startContainer(containerId)
    }
    StartContainersResponse()
  }


  def stopJobContainers(stopContainersRequest: StopContainersRequest): StopContainersResponse = {
    containersManager.stopContainer(0)
    StopContainersResponse()
  }

  def killJobContainers(killContainersRequest: KillContainersRequest): KillContainersResponse = {
    containersManager.killContainer(0)
    KillContainersResponse()
  }

  def downloadContainers(downloadContainersRequest: DownloadContainersRequest): DownloadContainersResponse = {
    val contents = downloadContainersRequest.containerIds.map { id =>
      val workspace = getContainerWorkspace(id)
      downloadContainersRequest.compressMethod match {
        case CompressMethod.ZIP =>
          if (workspace.exists)
            ContainerContent(id, zip(workspace), CompressMethod.ZIP)
          else
            ContainerContent(id, Array[Byte](), CompressMethod.ZIP)
        case _ =>
          throw new IllegalArgumentException(s"compress method not supported: ${downloadContainersRequest.compressMethod}")
      }
    }
    DownloadContainersResponse(sessionId = downloadContainersRequest.sessionId, containerContents = contents)
  }

  private def getContainerWorkspace(containerId: Long): Path = {
    containersDataDir / containerId.toString
  }
}

object ContainersServiceHandler {

  object CompressMethod {
    val ZIP = "zip"
  }

  def zip(path: Path): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val zipOutput = new ZipOutputStream(byteStream)
    try {
      path.walk.foreach(subPath => {
        if (Files.isRegularFile(subPath.jfile.toPath)) {
          val name = path.relativize(subPath).toString
          zipOutput.putNextEntry(new ZipEntry(name))
          val in = new FileInputStream(subPath.jfile)
          var bytesRead = in.read()
          while (bytesRead != -1) {
            zipOutput.write(bytesRead)
            bytesRead = in.read()
          }
          in.close()
          zipOutput.closeEntry()
        }
      })
    } finally {
      zipOutput.close()
    }
    byteStream.toByteArray
  }
}