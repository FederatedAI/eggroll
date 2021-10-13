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

import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, CoreConfKeys, NodeManagerConfKeys, ResourceManagerConfKeys, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils
import com.webank.eggroll.core.session.StaticErConf

// todo:2: args design
class Container(conf: RuntimeErConf, moduleName: String, processorId: Long = 0) extends Logging {
  // todo:1: constantize it
  private val confPrefix = s"eggroll.resourcemanager.bootstrap.${moduleName}"

  private val isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0

  private val bootStrapShell = conf.getString(CoreConfKeys.BOOTSTRAP_SHELL, if (isWindows) "C:\\Windows\\System32\\cmd.exe" else "/bin/bash")
  private val exeCmd = if (isWindows) "start /b python" else bootStrapShell
  private val bootStrapShellArgs = conf.getString(CoreConfKeys.BOOTSTRAP_SHELL_ARGS, if (isWindows) "/c" else "-c")
  private val exePath = conf.getString(s"${confPrefix}.exepath")
  private val sessionId = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)
  // todo:0: get from args instead of conf
  private val myServerNodeId = conf.getString(ResourceManagerConfKeys.SERVER_NODE_ID, "2")
  private val boot = conf.getString(CoreConfKeys.BOOTSTRAP_ROOT_SCRIPT, s"bin/eggroll_boot.${if(isWindows) "py" else "sh"}")
  private val logsDir = s"${CoreConfKeys.EGGROLL_LOGS_DIR.get()}"
  private val cmPort = conf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT)
  private val pythonPath = conf.getString(SessionConfKeys.EGGROLL_SESSION_PYTHON_PATH)
  private val pythonVenv = conf.getString(SessionConfKeys.EGGROLL_SESSION_PYTHON_VENV)

  if (StringUtils.isBlank(sessionId)) {
    throw new IllegalArgumentException("session Id is blank when creating processor")
  }

  def start(): Boolean = {
    var startCmd = ""
    var pythonPathArgs = ""
    var pythonVenvArgs = ""

    if (pythonPath.nonEmpty) {
      pythonPathArgs = s"--python-path ${pythonPath}"
    }

    if (pythonVenv.nonEmpty) {
      pythonVenvArgs = s"--python-venv ${pythonVenv}"
    }

    startCmd = s"""${exeCmd} ${boot} start "${exePath} --config ${conf.getString(CoreConfKeys.STATIC_CONF_PATH)} ${pythonPathArgs} ${pythonVenvArgs} --session-id ${sessionId} --server-node-id ${myServerNodeId} --processor-id ${processorId}" ${moduleName}-${processorId} &"""

//    if (pythonPath.isEmpty) {
//      startCmd = s"""${exeCmd} ${boot} start "${exePath} --config ${conf.getString(CoreConfKeys.STATIC_CONF_PATH)} --session-id ${sessionId} --server-node-id ${myServerNodeId} --processor-id ${processorId}" ${moduleName}-${processorId} &"""
//    } else {
//      startCmd = s"""${exeCmd} ${boot} start "${exePath} --config ${conf.getString(CoreConfKeys.STATIC_CONF_PATH)} --python-path ${pythonPath} --session-id ${sessionId} --server-node-id ${myServerNodeId} --processor-id ${processorId}" ${moduleName}-${processorId} &"""
//    }

    val standaloneTag = System.getProperty("eggroll.standalone.tag", StringConstants.EMPTY)
    logInfo(s"${standaloneTag} ${startCmd}")

    val thread = runCommand(startCmd)

    thread.start()
    thread.join()
    logInfo(s"start: ready to return: ${myServerNodeId}")
    thread.isAlive
  }

  def stop(): Boolean = {
    doStop(force = false)
  }

  def kill(): Boolean = {
    doStop(force = true)
  }

  private def doStop(force: Boolean = false): Boolean = {
    val op =  if (force) "kill" else "stop"
    val subCmd = if (isWindows) "None" else s"ps aux | grep 'session-id ${sessionId}' | grep 'server-node-id ${myServerNodeId}' | grep 'processor-id ${processorId}'"
    val doStopCmd = s"""${exeCmd} ${boot} ${op} \"${subCmd}\" ${moduleName}-${processorId}"""
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
      if(StringUtils.isNotBlank(System.getProperty("eggroll.standalone.tag"))) {
        logInfo(s"set EGGROLL_STANDALONE_PORT ${cmPort}")
        builderEnv.put("EGGROLL_STANDALONE_PORT", cmPort)
      }
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

