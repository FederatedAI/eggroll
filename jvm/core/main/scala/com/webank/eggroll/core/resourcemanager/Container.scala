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

package com.webank.eggroll.core.resourcemanager

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.lang.ProcessBuilder.Redirect

import com.webank.eggroll.core.constant.{CoreConfKeys, ResourceManagerConfKeys, SessionConfKeys}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

// todo:2: args design
class Container(conf: RuntimeErConf, moduleName: String, processorId: Long = 0) extends Logging {
  // todo:1: constantize it
  private val confPrefix = s"eggroll.resourcemanager.bootstrap.${moduleName}"

  private val isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") > 0

  private val bootStrapShell = conf.getString(CoreConfKeys.BOOTSTRAP_SHELL, if (isWindows) "c:\\windows\\cmd.exe" else "/bin/bash")
  private val bootStrapShellArgs = conf.getString(CoreConfKeys.BOOTSTRAP_SHELL_ARGS, if (isWindows) "\\c" else "-c")
  private val exePath = conf.getString(s"${confPrefix}.exepath")
  private val sessionId = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)
  // todo:0: get from args instead of conf
  private val myServerNodeId = conf.getString(ResourceManagerConfKeys.SERVER_NODE_ID, "2")
  private val boot = conf.getString(CoreConfKeys.BOOTSTRAP_ROOT_SCRIPT, s"bin/eggroll_boot.${if(isWindows) "bat" else "sh"}")
  private val logsDir = conf.getString(CoreConfKeys.LOGS_DIR)

  if (StringUtils.isBlank(sessionId)) {
    throw new IllegalArgumentException("session Id is blank when creating processor")
  }

  def start(): Boolean = {
    val startCmd = s"""${bootStrapShell} ${boot} start "${exePath} --config ${conf.getString(CoreConfKeys.STATIC_CONF_PATH)} --session-id ${sessionId} --server-node-id ${myServerNodeId} --processor-id ${processorId}" ${moduleName}-${processorId} &"""
    logInfo(s"${startCmd}")

    val thread = runCommand(startCmd)

    thread.start()
    thread.join()
    println("start: ready to return")
    thread.isAlive
  }

  def stop(): Boolean = {
    doStop(force = false)
  }

  def kill(): Boolean = {
    doStop(force = true)
  }

  private def doStop(force: Boolean = false): Boolean = {
    val subCmd =  if (force) "kill" else "stop"
    val doStopCmd = s"""${bootStrapShell} ${boot} ${subCmd} "ps aux | grep 'session-id ${sessionId}' | grep 'server-node-id ${myServerNodeId}' | grep 'processor-id ${processorId}'" ${moduleName}-${processorId}"""
    logInfo(doStopCmd)

    val thread = runCommand(doStopCmd)

    thread.start()
    thread.join()
    logInfo(s"${if (force) "killed" else "stopped"}")
    thread.isAlive
  }

  def runCommand(cmd: String): Thread = {
    new Thread(() => {
      val processorBuilder = new ProcessBuilder(bootStrapShell, bootStrapShellArgs, cmd)
      val builderEnv = processorBuilder.environment()
      val logPath = new File(s"${logsDir}${File.separator}${sessionId}${File.separator}")
      if(!logPath.exists()){
        logPath.mkdirs()
      }
      processorBuilder.redirectOutput(Redirect.appendTo(new File(logPath, s"bootstrap-${moduleName}-${processorId}.out")))
      processorBuilder.redirectError(Redirect.appendTo(new File(logPath, s"bootstrap-${moduleName}-${processorId}.err")))
      val process = processorBuilder.start()
      process.waitFor()
    })
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

