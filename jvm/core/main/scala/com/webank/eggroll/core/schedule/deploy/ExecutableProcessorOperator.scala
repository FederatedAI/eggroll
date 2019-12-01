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

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import java.util.concurrent.TimeUnit

import com.webank.eggroll.core.constant.{DeployConfKeys, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

// todo: abstract to general python starter
class ExecutableProcessorOperator(conf: RuntimeErConf) extends Logging {

  private val exePath = conf.getString("eggroll.node.exe")
  private val sessionId = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)
  private val nodeId = conf.getString("eggroll.node.id")
  private val logDir = conf.getString("eggroll.node.log.dir")
  private val boot = conf.getString("eggroll.boot.script","eggroll_processor." +
    (if(System.getProperty("os.name").toLowerCase().indexOf("windows") > 0) "bat" else "sh"))


  def start(): Boolean = {

    if (StringUtils.isBlank(sessionId)) {
      throw new IllegalArgumentException("session Id is blank when creating processor")
    }

    val startCmd = s"$boot start_node ${exePath} ${sessionId} $nodeId &"
    println(s"${startCmd}")

    val thread = new Thread(() => {
      val processorBuilder = new ProcessBuilder(boot, "start_node", exePath, sessionId, nodeId)
      // todo: 1. redirect output / error stream; 2. add session info; 3. add node manager
      val builderEnv = processorBuilder.environment()
      val logPath = new File(logDir + File.separator + sessionId + File.separator + nodeId)
      if(!logPath.exists()){
        logPath.mkdirs()
      }
      processorBuilder.redirectOutput(new File(logPath, "stdout.txt"))
      processorBuilder.redirectError(new File(logPath, "stderr.txt"))
      val process = processorBuilder.start()
      process.waitFor()
//      process.waitFor(1, TimeUnit.SECONDS)
    })

    thread.start()
    thread.join()
    println("ready to return")
    thread.isAlive
  }

  def stop(): Boolean = {
    val processBuilder = new ProcessBuilder(boot, "stop_node", exePath, sessionId, nodeId)

    val process = processBuilder.start()

    process.waitFor(1, TimeUnit.SECONDS)
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
