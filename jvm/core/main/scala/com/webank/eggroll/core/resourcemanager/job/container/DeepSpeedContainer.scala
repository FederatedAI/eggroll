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
                            conf: RuntimeErConf,
                            localRank: Int,
                            globalRank: Int,
                            commandArguments: Seq[String] = Seq.empty,
                            environmentVariables: Map[String, String] = Map.empty,
                            processorId: Long = 0,
                            files: Map[String, Array[Byte]] = Map.empty,
                            zippedFiles: Map[String, Array[Byte]] = Map.empty
                          ) {

  // working dir
  val workingDir: Path =
    Paths.get(conf.getString("eggroll.container.cwd", "/tmp/"))
      .resolve(processorId.toString)
      .toAbsolutePath.normalize()
  val workingDirectoryPreparer: Some[WorkingDirectoryPreparer] =
    Some(new WorkingDirectoryPreparer(
      files = files,
      zippedFiles = zippedFiles,
      workingDir = workingDir))

  val pythonExec: String = conf.getString("eggroll.container.python.exec")
  val scriptPath: String = conf.getString("eggroll.container.script.path")
  val extraEnv: Map[String, String] = environmentVariables ++ Map(
    "local_rank" -> localRank.toString,
    "global_rank" -> globalRank.toString
  )
  private val logDir = workingDir.resolve(
    Paths.get(conf.getString("eggroll.container.logs", "logs")))
  val stdErrFile: Some[Path] = Some(logDir.resolve(s"stderr.log"))
  val stdOutFile: Some[Path] = Some(logDir.resolve(s"stdout.log"))
}

class DeepSpeedContainer(config: DeepSpeedConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,
    scriptPath = config.scriptPath,
    scriptArgs = config.commandArguments,
    extraEnv = config.extraEnv,
    stdErrFile = config.stdErrFile,
    stdOutFile = config.stdOutFile,
    cwd = config.workingDir,
    workingDirectoryPreparer = config.workingDirectoryPreparer
  ) {
  def this(
            processorId: Long,
            conf: RuntimeErConf,
            localRank: Int,
            globalRank: Int,
            commandArguments: Seq[String] = Seq.empty,
            environmentVariables: Map[String, String] = Map.empty,
            files: Map[String, Array[Byte]] = Map.empty,
            zippedFiles: Map[String, Array[Byte]] = Map.empty) {
    this(DeepSpeedConfig(conf, localRank, globalRank, commandArguments, environmentVariables, processorId, files, zippedFiles))
  }
}


