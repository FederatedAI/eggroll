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
import com.webank.eggroll.core.containers.meta.DeepspeedContainerConfig
import com.webank.eggroll.core.session.StaticErConf

import java.nio.file.Path
import scala.collection.mutable


case class StoreConfig(
                        host: Option[String] = None,
                        port: Option[Int] = None,
                        prefix: String
                      ) {

  def getHost: String = {
    host.getOrElse(
      StaticErConf.getString(
        ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_STORE_HOST,
        StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST))
    )
  }

  def getPort: Int = {
    port.getOrElse(
      StaticErConf.getInt(
        ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_STORE_PORT,
        StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, -1))
    )
  }
}

case class WrapedDeepspeedContainerConfig(
                                           cudaVisibleDevices: Array[Int],
                                           worldSize: Int,
                                           crossRank: Int,
                                           crossSize: Int,
                                           localSize: Int,
                                           localRank: Int,
                                           rank: Int,
                                           storeConfig: StoreConfig,
                                           backend: Option[String] = None
                                         ) {

  def this(deepspeedContainerConfig: DeepspeedContainerConfig) = {
    this(
      cudaVisibleDevices = deepspeedContainerConfig.cudaVisibleDevices,
      worldSize = deepspeedContainerConfig.worldSize,
      crossRank = deepspeedContainerConfig.crossRank,
      crossSize = deepspeedContainerConfig.crossSize,
      localSize = deepspeedContainerConfig.localSize,
      localRank = deepspeedContainerConfig.localRank,
      rank = deepspeedContainerConfig.rank,
      storeConfig = StoreConfig(
        host = deepspeedContainerConfig.storeHost,
        port = deepspeedContainerConfig.storePort,
        prefix = deepspeedContainerConfig.storePrefix
      ),
      backend = deepspeedContainerConfig.backend
    )
  }

  private def getBackend: String = {
    backend.getOrElse(
      StaticErConf.getString(
        ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_BACKEND,
        "nccl"))
  }

  def getPytorchDistributedEnvironments: Map[String, String] = {
    Map(
      "WORLD_SIZE" -> worldSize.toString,
      "CROSS_RANK" -> crossRank.toString,
      "CROSS_SIZE" -> crossSize.toString,
      "LOCAL_SIZE" -> localSize.toString,
      "LOCAL_RANK" -> localRank.toString,
      "RANK" -> rank.toString,
      "CUDA_VISIBLE_DEVICES" -> cudaVisibleDevices.mkString(",")
    )
  }

  def getEggrollCustomizedEnvironments: Map[String, String] = {
    Map(
      "EGGROLL_DEEPSPEED_STORE_HOST" -> storeConfig.getHost,
      "EGGROLL_DEEPSPEED_STORE_PORT" -> storeConfig.getPort.toString,
      "EGGROLL_DEEPSPEED_STORE_PREFIX" -> storeConfig.prefix,
      "EGGROLL_DEEPSPEED_BACKEND" -> getBackend
    )
  }
}

case class DeepspeedContainerBuildConfig(
                                          sessionId: String,
                                          processorId: Long,
                                          containerWorkspace: scala.reflect.io.Path,
                                          deepspeedContainerConfig: WrapedDeepspeedContainerConfig,
                                          commandArguments: Seq[String] = Seq.empty,
                                          environmentVariables: Map[String, String] = Map.empty,
                                          files: Map[String, Array[Byte]] = Map.empty,
                                          zippedFiles: Map[String, Array[Byte]] = Map.empty,
                                          options: Map[String, String] = Map.empty
                                        ) {
  private val conf = new PythonContainerRuntimeConfig(options)
  val pythonExec: String = conf.getPythonExec(ContainerKey.DEEPSPEED_PYTHON_EXEC)

  // create boosting script to hook deepspeed initialization logic before user script
  private val runScript =
    f"""
       |import runpy
       |import pprint
       |import os
       |import sys
       |
       |from eggroll.deepspeed.boost import init_deepspeed
       |
       |print("===========current envs==============")
       |pprint.pprint(dict(os.environ))
       |print("===========current argv==============")
       |pprint.pprint(sys.argv)
       |
       |try:
       |    init_deepspeed()
       |except Exception as e:
       |    import traceback
       |
       |    print("===========init deepspeed failed=============")
       |    traceback.print_exc(file=sys.stdout)
       |    raise e
       |runpy.run_path("${conf.getString(ContainerKey.DEEPSPEED_SCRIPT_PATH)}", run_name='__main__')
       |
       |""".stripMargin.getBytes
  val scriptPath = "_boost.py"

  // working dir
  val workingDir: Path = containerWorkspace.jfile.toPath
  val workingDirectoryPreparer: Some[WorkingDirectoryPreparer] =
    Some(new WorkingDirectoryPreparer(
      files = files ++ Map(scriptPath -> runScript),
      zippedFiles = zippedFiles,
      workingDir = workingDir))
  val (stdErrFile, stdOutFile) = {
    val logDir = workingDir.resolve("logs")
    (Some(logDir.resolve(s"stderr.log")), Some(logDir.resolve(s"stdout.log")))
  }

  val extraEnv: Map[String, String] = {
    val mutableEnv = mutable.Map.empty[String, String]
    mutableEnv ++= environmentVariables
    // pytorch distributed related envs override user envs
    for ((k, v) <- deepspeedContainerConfig.getPytorchDistributedEnvironments ++
      deepspeedContainerConfig.getEggrollCustomizedEnvironments) {
      mutableEnv += (k -> v)
    }
    // add container dir
    mutableEnv += ("EGGROLL_DEEPSPEED_CONTAINER_DIR" -> workingDir.toAbsolutePath.toString)
    // read `EGGROLL_HOME` and `PYTHONPATH` from system env since this is node level env
    sys.env.get("EGGROLL_HOME") match {
      case Some(home) =>
        mutableEnv += ("EGGROLL_HOME" -> home)
        // add eggroll python path
        if (sys.env.contains("PYTHONPATH")) {
          mutableEnv += ("PYTHONPATH" -> s"${sys.env("PYTHONPATH")}:$home/python")
        }
        else {
          mutableEnv += ("PYTHONPATH" -> s"$home/python")
        }
      case None => throw new Exception("EGGROLL_HOME not set")
    }
    mutableEnv.toMap
  }
}


class DeepSpeedContainer(config: DeepspeedContainerBuildConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,
    scriptPath = config.scriptPath,
    scriptArgs = config.commandArguments,
    extraEnv = config.extraEnv,
    stdErrFile = config.stdErrFile,
    stdOutFile = config.stdOutFile,
    cwd = config.workingDir,
    workingDirectoryPreparer = config.workingDirectoryPreparer,
    processorId = config.processorId) {
  def this(
            sessionId: String,
            processorId: Long,
            deepspeedContainerConfig: WrapedDeepspeedContainerConfig,
            containerWorkspace: scala.reflect.io.Path,
            commandArguments: Seq[String] = Seq.empty,
            environmentVariables: Map[String, String] = Map.empty,
            files: Map[String, Array[Byte]] = Map.empty,
            zippedFiles: Map[String, Array[Byte]] = Map.empty,
            options: Map[String, String] = Map.empty
          ) {
    this(DeepspeedContainerBuildConfig(sessionId = sessionId,
      processorId = processorId,
      containerWorkspace = containerWorkspace,
      deepspeedContainerConfig = deepspeedContainerConfig,
      commandArguments = commandArguments,
      environmentVariables = environmentVariables,
      files = files,
      zippedFiles = zippedFiles,
      options = options
    ))
  }

  override def preStart(): Unit = {
    super.preStart()
    logInfo(s"prepare DeepSpeedContainer start: $config")
  }
}


