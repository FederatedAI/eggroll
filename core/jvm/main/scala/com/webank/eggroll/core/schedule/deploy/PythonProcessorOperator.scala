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

package com.webank.eggroll.core.schedule.deploy

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util
import java.util.concurrent.TimeUnit

import com.webank.eggroll.core.constant.{DeployConfKeys, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

// todo: abstract to general python starter
class PythonProcessorOperator() extends Logging {

  private val commands = new util.LinkedList[String]()

  def start(conf: RuntimeErConf): Boolean = {
    val venv = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH)
    val dataDir = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH)
    val eggPair = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH)
    val pythonPath = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH)
    val nodeManagerPort = conf.getInt(DeployConfKeys.CONFKEY_DEPLOY_NODE_MANAGER_PORT, 9394)
    val sessionId = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)

    if (StringUtils.isBlank(sessionId)) {
      throw new IllegalArgumentException("session Id is blank when creating processor")
    }

    commands.add("echo $PYTHONPATH")
    commands.add(s"source ${venv}/bin/activate")

    commands.add(s"echo '${venv}/bin/python ${eggPair} -d ${dataDir}'")
    commands.add(s"${venv}/bin/python ${eggPair} -d ${dataDir} -n ${nodeManagerPort} -s ${sessionId} &")

    val processorBuilder: ProcessBuilder = new ProcessBuilder("/bin/bash", "-c", String.join(StringConstants.SEMICOLON, commands))

    // todo: 1. redirect output / error stream; 2. add session info; 3. add node manager
    val builderEnv = processorBuilder.environment()
    builderEnv.put("PYTHONPATH", s"${pythonPath}")

    val process = processorBuilder.start()

    process.waitFor(100, TimeUnit.MILLISECONDS)
  }

  def stop(port: Int): Boolean = {
    val processBuilder = new ProcessBuilder("/bin/bash", "-c", s"lsof -t -i:${port} | xargs kill")

    val process = processBuilder.start()

    process.waitFor(1, TimeUnit.SECONDS)
    process.exitValue() == 0
  }

  def stop(processor: ErProcessor): Boolean = {
    // todo: check if this processor belong to me (i.e. checking my ip address)
    val port = processor.commandEndpoint.port
    stop(port)
  }


  @throws[Exception]
  def loadStream(s: InputStream): String = {
    val br: BufferedReader = new BufferedReader(new InputStreamReader(s))
    val sb: StringBuilder = new StringBuilder
    var line: String = null
    line = br.readLine()
    while (line != null) {
      sb.append(line).append("\n")
      line = br.readLine()
    }
    sb.toString
  }
}
