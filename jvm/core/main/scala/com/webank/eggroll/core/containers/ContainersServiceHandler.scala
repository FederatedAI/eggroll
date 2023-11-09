package com.webank.eggroll.core.containers

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ProcessorStatus}
import com.webank.eggroll.core.containers.ContainersServiceHandler.{CompressMethod, LogStreamHolder, logError, zip}
import com.webank.eggroll.core.containers.container.{ContainersManager, DeepSpeedContainer, WarpedDeepspeedContainerConfig}
import com.webank.eggroll.core.containers.meta._
import com.webank.eggroll.core.error.PathNotExistException
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.resourcemanager.NodeManagerMeta
import com.webank.eggroll.core.session
import com.webank.eggroll.core.session.{ExtendEnvConf, StaticErConf}
import com.webank.eggroll.core.transfer.Extend
import com.webank.eggroll.core.util.{Logging, ProcessUtils}
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import java.io.{BufferedReader, ByteArrayOutputStream, FileInputStream, InputStreamReader}
import java.nio.file.Files
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.io.Path
import scala.util.control.Breaks.{break, breakable}


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
      var  envMap :Map[String, String] =startDeepspeedContainerRequest.environmentVariables.++(ExtendEnvConf.getAll)
      logInfo("containerId "+containerId+"env map : "+envMap)
      val container = new DeepSpeedContainer(
        sessionId = sessionId,
        processorId = containerId,
        deepspeedContainerConfig = new WarpedDeepspeedContainerConfig(deepspeedConfig),
        containerWorkspace = getContainerWorkspace(sessionId, deepspeedConfig.rank),
        commandArguments = startDeepspeedContainerRequest.commandArguments,
        environmentVariables =envMap ,
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
    val ranks = downloadContainersRequest.ranks
    logInfo(s"(sessionId=$sessionId)downloading containers: ${ranks.mkString(",")}")

    val contents = ranks.map { rank =>
      val targetDir = containerContentType match {
        case ContentType.ALL => getContainerWorkspace(sessionId, rank)
        case ContentType.MODELS => getContainerModelsDir(sessionId, rank)
        case ContentType.LOGS => getContainerLogsDir(sessionId, rank)
        case _ => throw new IllegalArgumentException(s"unsupported container content type: $containerContentType")
      }
      downloadContainersRequest.compressMethod match {
        case CompressMethod.ZIP =>
          if (targetDir.exists)
            ContainerContent(rank, zip(targetDir, downloadContainersRequest.compressLevel), CompressMethod.ZIP)
          else
            ContainerContent(rank, Array[Byte](), CompressMethod.ZIP)
        case _ =>
          throw new IllegalArgumentException(s"compress method not supported: ${downloadContainersRequest.compressMethod}")
      }
    }
    DownloadContainersResponse(sessionId = downloadContainersRequest.sessionId, containerContents = contents)
  }

  private def getContainerWorkspace(sessionId: String, rank: Long): Path = {
    containersDataDir / sessionId / rank.toString
  }

  private def getContainerModelsDir(sessionId: String, rank: Long): Path = {
    getContainerWorkspace(sessionId, rank) / MODELS
  }

  private def getContainerLogsDir(sessionId: String, rank: Long): Path = {
    getContainerWorkspace(sessionId, rank) / LOGS
  }




  def  createLogStream(request: Extend.GetLogRequest, responseObserver: StreamObserver[Extend.GetLogResponse]): LogStreamHolder =synchronized{
//    tail -n 1000：显示最后1000行
//    tail -n +1000：从1000行开始显示，显示1000行以后的
//    head -n 1000：显示前面1000行
//

      var sessionId = request.getSessionId
      var line =  if(request.getStartLine>0) request.getStartLine else "+0"

      var rank = request.getRank
      var path: Path = getContainerLogsDir(sessionId, rank.toLong)
      path = path / (if(StringUtils.isNotEmpty(request.getLogType))request.getLogType else "INFO" ) + ".log"

      if (!path.exists) {
        throw new PathNotExistException(s"can not found file ${path}")
      }
      var command ="tail -F -n  "+line +" "+ path
      var logStreamHolder = new LogStreamHolder(System.currentTimeMillis(), "tail -F -n  "+line +" "+ path, responseObserver, "running")
      return logStreamHolder

  }




}

object ContainersServiceHandler extends Logging {

  var  singleton : ContainersServiceHandler=null

  def  getOrCreate(executionContext: ExecutionContext):ContainersServiceHandler = synchronized{
    if(singleton!=null){
       singleton
    }else{
      singleton =  new ContainersServiceHandler()(executionContext)
      singleton
    }
  }

  def get(): ContainersServiceHandler ={
     singleton;
  }




  class LogStreamHolder(createTimestamp:Long,
                         command:String,
                        streamObserver: StreamObserver[Extend.GetLogResponse],
                        var status: String){

    var thread:Thread = null;
    var  bufferReader:BufferedReader= null
    var process:Process=null;
      def  stop():Unit={
        logInfo("receive stop log stream command")
        this.status = "stop"
        thread.interrupt()
        process.destroyForcibly()
        bufferReader.close()

      }

      def  run(): Unit ={
        logInfo(s"log begin to run")
        thread = new Thread(()=>{
          try {
            logInfo(s"log stream begin ${command}")
            process = ProcessUtils.createProcess(command)
            bufferReader= new BufferedReader(new InputStreamReader(process.getInputStream()))
            var batchSize = 10;
            breakable {
              while (true&& !Thread.interrupted()) {
                if(status=="stop"){
                  break;
                }
                var logBuffer = new ArrayBuffer[String]()
                var line: String = bufferReader.readLine()
                if(line!=null){
                  logBuffer.append(line)
                }

                if (logBuffer.size > 0) {
                  var response: Extend.GetLogResponse = Extend.GetLogResponse.newBuilder().setCode("0").addAllDatas(logBuffer.toList.asJava).build()
                  streamObserver.onNext(response)
                } else {
                  Thread.sleep(1000)
                }
              }
            }
          }catch{
            case t: InterruptedException =>
            case t: Throwable => logError("start log stream error",t)
          }
          finally {
            if(bufferReader!=null){
              bufferReader.close()

            }
            if(process!=null)
              process.destroyForcibly()
            if(streamObserver!=null) {
              try{
                streamObserver.onCompleted()
              }catch{
                case t: Throwable =>
                  logError("send onCmpleted error")
              }
            }
            logInfo("log stream destroy over")
          }

        })
        thread .start()
      }

  }


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