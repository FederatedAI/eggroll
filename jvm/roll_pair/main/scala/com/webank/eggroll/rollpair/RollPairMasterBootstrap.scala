package com.webank.eggroll.rollpair

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

import _root_.io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErProcessor}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang3.StringUtils
import java.util
import com.sun.jna.platform.win32.Kernel32
import com.sun.jna.platform.win32.WinBase
import com.sun.jna.platform.win32.WinError
import com.sun.jna.platform.win32.WinNT
import com.sun.jna.ptr.IntByReference

class stopProcessor(clusterManagerClient:ClusterManagerClient, myself:ErProcessor) extends Thread with Logging {
  def assertValidHandle(message: String, handle: WinNT.HANDLE): WinNT.HANDLE = {
    if ((handle == null) || WinBase.INVALID_HANDLE_VALUE == handle) {
      val hr = Kernel32.INSTANCE.GetLastError
      if (hr == WinError.ERROR_SUCCESS) logInfo(message + " failed with unknown reason code")
      else logInfo(message + " failed: hr=" + hr + " - 0x" + Integer.toHexString(hr))
    }
    handle
  }

  def assertCallSucceeded(message: String, result: Boolean): Unit = {
    if (result) return
    val hr = Kernel32.INSTANCE.GetLastError
    if (hr == WinError.ERROR_SUCCESS) logInfo(message + " failed with unknown reason code")
    else logInfo(message + " failed: hr=" + hr + " - 0x" + Integer.toHexString(hr))
  }

  override def run(){
    val name = ManagementFactory.getRuntimeMXBean().getName()
    logInfo(name)
    val pid = name.split("@")(0)

    val pipeName = "\\\\.\\pipe\\pid_pipe" + pid
    val pipe_buffer_size = 1024
    val hNamedPipe = assertValidHandle("CreateNamedPipe",
      Kernel32.INSTANCE.CreateNamedPipe(pipeName, WinBase.PIPE_ACCESS_DUPLEX,
        WinBase.PIPE_TYPE_MESSAGE | WinBase.PIPE_READMODE_MESSAGE | WinBase.PIPE_WAIT,
        1,
        pipe_buffer_size,
        pipe_buffer_size,
        TimeUnit.SECONDS.toMillis(30L).toInt,
        null))

    try {
      logInfo("Await client connection")
      assertCallSucceeded("ConnectNamedPipe", Kernel32.INSTANCE.ConnectNamedPipe(hNamedPipe, null))
      logInfo("Client connected")
      val readBuffer = new Array[Byte](pipe_buffer_size)
      val lpNumberOfBytesRead = new IntByReference(0)
      assertCallSucceeded("ReadFile", Kernel32.INSTANCE.ReadFile(hNamedPipe, readBuffer, readBuffer.length, lpNumberOfBytesRead, null))
      val readSize = lpNumberOfBytesRead.getValue
      logInfo("Received client data - length=" + readSize)
      val readBufferCut = util.Arrays.copyOfRange(readBuffer, 0, readSize)
      System.out.println(util.Arrays.toString(readBufferCut))
      val receive_msg = new String(readBufferCut,"ascii")
      System.out.println(receive_msg)

      // Flush the pipe to allow the client to read the pipe's contents before disconnecting
      assertCallSucceeded("FlushFileBuffers", Kernel32.INSTANCE.FlushFileBuffers(hNamedPipe))
      logInfo("Disconnecting")
      assertCallSucceeded("DisconnectNamedPipe", Kernel32.INSTANCE.DisconnectNamedPipe(hNamedPipe))
      logInfo("Disconnected")

      if (receive_msg.indexOf("stop")>=0 && receive_msg.indexOf(pid)>=0) {
        System.out.println("receive msg")
        val terminatedSelf = myself.copy(status = ProcessorStatus.STOPPED)
        clusterManagerClient.heartbeat(terminatedSelf)
      }
    } finally {
      // clean up
      assertCallSucceeded("Named pipe handle close", Kernel32.INSTANCE.CloseHandle(hNamedPipe))
    }

    logInfo("Thread is running?");

  }
}

class RollPairMasterBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var sessionId = "er_session_null"
  private var nodeManager = ""
  private var args: Array[String] = _
  private var cmd: CommandLine = null
  private var cmHost = ""
  private var cmPort = 0
  private var nmPort = ""

  override def init(args: Array[String]): Unit = {
    this.args = args
    cmd = CommandArgsUtils.parseArgs(args)
    sessionId = cmd.getOptionValue('s')
    val cm = cmd.getOptionValue("cm")
    nmPort = cmd.getOptionValue("nm")
    if (cm != null) {
      val toks = cm.split(":")
      cmHost = toks(0)
      cmPort = toks(1).toInt
      StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, cmHost)
      StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, cmPort.toString)
    }

    CommandRouter.register(serviceName = RollPair.ROLL_RUN_JOB_COMMAND.uriString,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPair.RUN_JOB)
  }

  def reportCM(sessionId: String, args: Array[String], myCommandPort: Int):Unit = {
    // todo:2: heartbeat service
    val portString = cmd.getOptionValue('p', "0")
    val clusterManager = cmd.getOptionValue("cluster-manager", "localhost:4670")
    val nodeManager = cmd.getOptionValue("node-manager", "localhost:9394")
    val serverNodeId = cmd.getOptionValue("server-node-id", "0").toLong
    val confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    val processorId = cmd.getOptionValue("processor-id", "0").toLong

    val clusterManagerClient = new ClusterManagerClient(ErEndpoint(clusterManager))

    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    val processName = ManagementFactory.getRuntimeMXBean.getName
    val pid = processName.split(StringConstants.AT, 2)(0).toInt
    val myself = ErProcessor(
      id = processorId,
      serverNodeId = serverNodeId,
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      commandEndpoint = ErEndpoint("localhost", myCommandPort),
      transferEndpoint = ErEndpoint("localhost", myCommandPort),
      pid = pid,
      options = options,
      status = ProcessorStatus.RUNNING)
    logInfo("ready to heartbeat")

    val isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0
    if (isWindows) {
      val t = new stopProcessor(clusterManagerClient, myself)
      t.start()
    }

    clusterManagerClient.heartbeat(myself)

    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, cmPort.toString)
    StaticErConf.addProperty(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, nmPort)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logInfo(s"*** roll pair master exit gracefully. sessionId: ${sessionId}, serverNodeId: ${serverNodeId}, processorId: ${processorId}, port: ${portString} ***")
        val terminatedSelf = myself.copy(status = ProcessorStatus.STOPPED)
        clusterManagerClient.heartbeat(terminatedSelf)
        this.interrupt()
      }
    })
  }

  override def start(): Unit = {
    val managerEndpoint = if (StringUtils.isBlank(nodeManager)) {
      ErEndpoint(host = "localhost", port = 9394)
    } else {
      val splittedManager = nodeManager.trim.split(StringConstants.COLON, 2)
      println(splittedManager)
      println(splittedManager.length)

      val managerHost = if (splittedManager.length == 1) "localhost" else splittedManager(0)
      val managerPort = if (splittedManager.length == 1) splittedManager(0) else splittedManager(1)

      ErEndpoint(host = managerHost, port = managerPort.toInt)
      StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, managerPort)
    }
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    this.port = cmd.getOptionValue('p', "0").toInt

    val server = GrpcServerUtils.createServer(
      port = this.port, grpcServices = List(new CommandService))

    server.start()
    this.port = server.getPort
    StaticErConf.setPort(port)
    logInfo(s"server started at ${port}")
    // job
    reportCM(sessionId, args, port)

    logInfo("heartbeated")
  }
}
