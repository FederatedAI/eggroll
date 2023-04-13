package com.webank.eggroll.core.resourcemanager.job.container

import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.session.RuntimeErConf

import java.io.File
import java.nio.file.{Path, Paths}

object EggPairConfKeys {
  val VENV_KEY = "eggroll.resourcemanager.bootstrap.egg_pair.venv"
  val FILEPATH_KEY = "eggroll.resourcemanager.bootstrap.egg_pair.filepath"
  val PYTHONPATH_KEY = "eggroll.resourcemanager.bootstrap.egg_pair.pythonpath"
  val MALLOC_MMAP_THRESHOLD_KEY= "eggroll.core.malloc.mmap.threshold"
  val MALLOC_MMAP_MAX_KEY = "eggroll.core.malloc.mmap.max"
}

import com.webank.eggroll.core.resourcemanager.job.container.EggPairConfKeys._

case class EggPairConfig(conf: RuntimeErConf, processorId: Long) {
  val EGGROLL_HOME = sys.env("EGGROLL_HOME")
  val pythonExec: String = Option(conf.getString(VENV_KEY)).filter(_.nonEmpty).map(s => s"$s/bin/python").getOrElse("python")
  val scriptPath: String = conf.getString(FILEPATH_KEY)
  val pythonPath: String = conf.getString(PYTHONPATH_KEY)

  val sessionId: String = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)
  val serverNodeId: String = conf.getString(ResourceManagerConfKeys.SERVER_NODE_ID, "2")

  val clusterManagerHost: String = conf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST)
  val clusterManagerPort: String = conf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT)
  val nodeManagerPort: String = conf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT)

  val scriptArgs: Seq[String] = Seq(
    "--config", conf.getString(CoreConfKeys.STATIC_CONF_PATH),
    "--session-id", sessionId,
    "--server-node-id", serverNodeId,
    "--cluster-manager", s"${clusterManagerHost}:${clusterManagerPort}",
    "--node-manager", s"${nodeManagerPort}",
    "--processor-id", processorId.toString
  )

  val logsDir: Path = Paths.get(conf.getString(CoreConfKeys.EGGROLL_LOGS_DIR.get(), Paths.get(EGGROLL_HOME, "logs").toString), sessionId)
  val extraEnv: Map[String, String] = Seq(
    ("EGGROLL_HOME", EGGROLL_HOME),
    ("PYTHONPATH", pythonPath),
    ("MALLOC_MMAP_THRESHOLD_", conf.getString(MALLOC_MMAP_THRESHOLD_KEY)),
    ("MALLOC_MMAP_MAX_", conf.getString(MALLOC_MMAP_MAX_KEY)),
    ("EGGROLL_LOGS_DIR", s"${logsDir}"),
    ("EGGROLL_LOG_FILE", s"egg_pair-${processorId}")
  ).toMap

  val stdErrFile: File = logsDir.resolve(s"egg_pair-${processorId}.err").toFile
  val stdOutFile: File = logsDir.resolve(s"egg_pair-${processorId}.out").toFile
}

class EggPairContainer(config: EggPairConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,
    scriptPath = config.scriptPath,
    scriptArgs = config.scriptArgs,
    extraEnv = config.extraEnv,
    stdErrFile = Some(config.stdErrFile),
    stdOutFile = Some(config.stdOutFile)
  ){
  def this(conf: RuntimeErConf, processorId: Long = 0) = {
    this(EggPairConfig(conf, processorId))
  }
}