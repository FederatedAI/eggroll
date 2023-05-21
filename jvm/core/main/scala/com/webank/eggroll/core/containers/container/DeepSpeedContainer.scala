/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.containers.container

import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}

import java.nio.file.{Path, Paths}

case class DeepSpeedConfig(
                            jobId: String,
                            conf: PythonContainerRuntimeConfig,
                            localRank: Int,
                            globalRank: Int,
                            worldSize: Int,
                            commandArguments: Seq[String] = Seq.empty,
                            environmentVariables: Map[String, String] = Map.empty,
                            processorId: Long = 0,
                            files: Map[String, Array[Byte]] = Map.empty,
                            zippedFiles: Map[String, Array[Byte]] = Map.empty
                          ) {
  val pythonExec: String = conf.getPythonExec(ContainerKey.DEEPSPEED_PYTHON_EXEC)

  // working dir
  val workingDir: Path =
    Paths.get(conf.getString(ContainerKey.WORKING_DIR, "/tmp/"))
      .resolve(processorId.toString)
      .toAbsolutePath.normalize()

  // create boosting script to hook deepspeed initialization logic before user script
  val storeHost = conf.getString(
    ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_STORE_HOST,
    StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST))
  require(storeHost.nonEmpty)
  val storePort = conf.getInt(
    ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_STORE_PORT,
    StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, -1))
  require(storePort > 0)
  private val runScript = DeepSpeedRunPy.runPy(
    sessionId = jobId,
    scriptPath = conf.getString(ContainerKey.DEEPSPEED_SCRIPT_PATH),
    storeHost = storeHost,
    storePort = storePort,
    worldSize = worldSize,
    rank = globalRank,
    backend = conf.getString(ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_BACKEND, "nccl")
  )
  val workingDirectoryPreparer: Some[WorkingDirectoryPreparer] =
    Some(new WorkingDirectoryPreparer(
      files = files ++ Map(DeepSpeedRunPy.runPyName -> runScript),
      zippedFiles = zippedFiles,
      workingDir = workingDir))

  val eggrollHome = conf.getString("eggroll.home") // TODO: this should removed in someday
  val extraEnv: Map[String, String] = environmentVariables ++ Map(
    "LOCAL_RANK" -> localRank.toString,
    "GLOBAL_RANK" -> globalRank.toString,
    "EGGROLL_HOME" -> eggrollHome,
    "PYTHONPATH" -> s"$eggrollHome/python"  //TODO: append to existing PYTHONPATH?
  )
  private val logDir = workingDir.resolve(
    Paths.get(conf.getString(ContainerKey.LOGS_DIR, "logs")))
  val stdErrFile: Some[Path] = Some(logDir.resolve(s"stderr.log"))
  val stdOutFile: Some[Path] = Some(logDir.resolve(s"stdout.log"))
}

object DeepSpeedRunPy {
  def runPy(sessionId: String,
            scriptPath: String,
            storeHost: String,
            storePort: Int,
            worldSize: Int,
            rank: Int,
            backend: String): Array[Byte] = {

    f"""
       |def main(session_id, script_path, store_host, store_port, world_size, rank, backend):
       |    import runpy
       |    from eggroll.deepspeed.store.client import EggrollStore
       |    from torch import distributed
       |
       |    store = EggrollStore(store_host, store_port, session_id)
       |    distributed.init_process_group(backend=backend, store=store, world_size=world_size, rank=rank)
       |    runpy.run_path(script_path, run_name='__main__')
       |
       |
       |if __name__ == '__main__':
       |    main(
       |        session_id="${sessionId}",
       |        script_path="${scriptPath}",
       |        store_host="${storeHost}",
       |        store_port="${storePort}",
       |        world_size=${worldSize},
       |        rank=${rank},
       |        backend="${backend}"
       |    )
       |
       |""".stripMargin.getBytes
  }

  val runPyName: String = "_run.py"

}


class DeepSpeedContainer(containerId: String, config: DeepSpeedConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,

    scriptPath = DeepSpeedRunPy.runPyName,
    scriptArgs = config.commandArguments,
    extraEnv = config.extraEnv,
    stdErrFile = config.stdErrFile,
    stdOutFile = config.stdOutFile,
    cwd = config.workingDir,
    workingDirectoryPreparer = config.workingDirectoryPreparer,
    containerId = containerId,
    processorId = config.processorId
  ) {
  def this(
            containerId: String,
            jobId: String,
            processorId: Long,
            conf: RuntimeErConf,
            localRank: Int,
            globalRank: Int,
            worldSize: Int,
            commandArguments: Seq[String] = Seq.empty,
            environmentVariables: Map[String, String] = Map.empty,
            files: Map[String, Array[Byte]] = Map.empty,
            zippedFiles: Map[String, Array[Byte]] = Map.empty) {
    this(containerId, DeepSpeedConfig(jobId, new PythonContainerRuntimeConfig(conf), localRank, globalRank, worldSize, commandArguments, environmentVariables, processorId, files, zippedFiles))
  }

  override def preStart(): Unit = {
    super.preStart()
    logInfo(s"prepare DeepSpeedContainer start: ${config}")
  }
}


