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

import java.nio.file.Paths

case class DeepSpeedConfig(conf: RuntimeErConf, localRank: Int, globalRank: Int, processorId: Long = 0) {
  val pythonExec = conf.getString("deepspeed.python.exec")
  val scriptPath = conf.getString("deepspeed.script.path")
  val scriptArgs = conf.getString("deepspeed.script.args").split(",")
  val extraEnv = Map(
    "local_rank" -> localRank.toString,
    "global_rank" -> globalRank.toString
  )
  private val logDir = Paths.get(conf.getString("deepspeed.logdir", "/tmp"))
  val stdErrFile = Some(logDir.resolve(s"deepspeed-stderr-$processorId.log").toFile)
  val stdOutFile = Some(logDir.resolve(s"deepspeed-stdout-$processorId.log").toFile)
}

class DeepSpeedContainer(config: DeepSpeedConfig)
  extends PythonContainer(
    pythonExec = config.pythonExec,
    scriptPath = config.scriptPath,
    scriptArgs = config.scriptArgs,
    extraEnv = config.extraEnv,
    stdErrFile = config.stdErrFile,
    stdOutFile = config.stdOutFile
  ) {
  def this(conf: RuntimeErConf, localRank: Int, globalRank: Int, processor_id: Long = 0) {
    this(DeepSpeedConfig(conf, localRank, globalRank, processor_id))
  }
}


