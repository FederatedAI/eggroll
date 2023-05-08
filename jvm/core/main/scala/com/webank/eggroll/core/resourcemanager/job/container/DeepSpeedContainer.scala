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

package com.webank.eggroll.core.resourcemanager.job.container

import com.webank.eggroll.core.session.RuntimeErConf

import java.nio.file.{Path, Paths}

case class DeepSpeedConfig(
                            jobId: String,
                            conf: PythonContainerRuntimeConfig,
                            localRank: Int,
                            globalRank: Int,
                            worldSize: Int,
                            storeHost: String,
                            storePort: Int,
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
  private val runScript = DeepSpeedRunPy.runPy(
    sessionId = jobId,
    scriptPath = conf.getString(ContainerKey.DEEPSPEED_SCRIPT_PATH),
    eggrollHome = conf.getString("eggroll.home"),
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

  val extraEnv: Map[String, String] = environmentVariables ++ Map(
    "LOCAL_RANK" -> localRank.toString,
    "GLOBAL_RANK" -> globalRank.toString
  )
  private val logDir = workingDir.resolve(
    Paths.get(conf.getString(ContainerKey.LOGS_DIR, "logs")))
  val stdErrFile: Some[Path] = Some(logDir.resolve(s"stderr.log"))
  val stdOutFile: Some[Path] = Some(logDir.resolve(s"stdout.log"))
}

object DeepSpeedRunPy {
  def runPy(sessionId: String,
            scriptPath: String,
            eggrollHome: String,
            storeHost: String,
            storePort: Int,
            worldSize: Int,
            rank: Int,
            backend: String): Array[Byte] =
    f"""
       |import os
       |import runpy
       |import sys
       |from datetime import timedelta
       |
       |from torch import distributed
       |from torch.distributed import Store
       |
       |
       |def main(session_id, script_path, eggroll_home, store_host, store_port, world_size, rank, backend="nccl"):
       |    sys.path.append(f"{eggroll_home}/python")
       |    os.environ["EGGROLL_HOME"] = eggroll_home
       |
       |    from eggroll.core.client import ClusterManagerClient
       |    from eggroll.core.deepspeed.store_model import RendezvousStoreSetRequest, RendezvousStoreGetRequest, RendezvousStoreAddRequest
       |
       |    class EggrollStore(Store):
       |        def __init__(self, host, port, prefix, timeout: timedelta = timedelta(seconds=300)):
       |            self._host = host
       |            self._port = port
       |            self._prefix = prefix
       |            self._timeout = timeout
       |            super().__init__()
       |
       |        def get(self, key, timeout: timedelta = None):
       |            if isinstance(key, str):
       |                key = key.encode()
       |            if timeout is None:
       |                timeout = self._timeout
       |            cluster_manager_client = ClusterManagerClient(options={})
       |            get_request = RendezvousStoreGetRequest(prefix=self._prefix, key=key, timeout=timeout)
       |            response = cluster_manager_client.rendezvous_store_get(get_request)
       |            return response.value
       |
       |        def set(self, key, value):
       |            if isinstance(key, str):
       |                key = key.encode()
       |            cluster_manager_client = ClusterManagerClient(options={})
       |            set_request = RendezvousStoreSetRequest(prefix=self._prefix, key=key, value=value)
       |            return cluster_manager_client.rendezvous_store_set(set_request)
       |
       |        def add(self, key, amount):
       |            if isinstance(key, str):
       |                key = key.encode()
       |            cluster_manager_client = ClusterManagerClient(options={})
       |            add_request = RendezvousStoreAddRequest(prefix=self._prefix, key=key, amount=amount)
       |            response = cluster_manager_client.rendezvous_store_add(add_request)
       |            return response.amount
       |
       |
       |    store = EggrollStore(store_host, store_port, "session_id")
       |    distributed.init_process_group(backend=backend, store=store, world_size=world_size, rank=rank)
       |    runpy.run_path(script_path, run_name='__main__')
       |
       |
       |if __name__ == '__main__':
       |    main(
       |        session_id="${sessionId}",
       |        script_path="${scriptPath}",
       |        eggroll_home="${eggrollHome}",
       |        store_host="${storeHost}",
       |        store_port="${storePort}",
       |        world_size=${worldSize},
       |        rank =${rank},
       |        backend = "${backend}"
       |    )
       |
       |""".stripMargin.getBytes

  val runPyName: String = "_run.py"
}


class DeepSpeedContainer( containerId : String, config: DeepSpeedConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,
    scriptPath = DeepSpeedRunPy.runPyName,
    scriptArgs = config.commandArguments,
    extraEnv = config.extraEnv,
    stdErrFile = config.stdErrFile,
    stdOutFile = config.stdOutFile,
    cwd = config.workingDir,
    workingDirectoryPreparer = config.workingDirectoryPreparer
    containerId = containerId,
    processorId = config.processorId
  ) {
  def this(
            jobId: String,
            processorId: Long,
            conf: RuntimeErConf,
            localRank: Int,
            globalRank: Int,
            worldSize: Int,
            storeHost: String,
            storePort: Int,
            commandArguments: Seq[String] = Seq.empty,
            environmentVariables: Map[String, String] = Map.empty,
            files: Map[String, Array[Byte]] = Map.empty,
            zippedFiles: Map[String, Array[Byte]] = Map.empty) {
    this(DeepSpeedConfig(jobId, new PythonContainerRuntimeConfig(conf), localRank, globalRank, worldSize, storeHost, storePort, commandArguments, environmentVariables, processorId, files, zippedFiles))
  }

  override def preStart(): Unit = {
    super.preStart()
    logInfo(s"prepare DeepSpeedContainer start: ${config}")
  }
}


