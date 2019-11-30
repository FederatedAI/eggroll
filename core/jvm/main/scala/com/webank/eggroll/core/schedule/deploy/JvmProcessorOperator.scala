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

import java.io.{BufferedReader, File, FileOutputStream, InputStream, InputStreamReader}
import java.util
import java.util.concurrent.TimeUnit

import com.webank.eggroll.core.constant.{DeployConfKeys, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

// todo: abstract to general python starter
class JvmProcessorOperator() extends Logging {

  private val commands = new util.LinkedList[String]()

  def start(conf: RuntimeErConf): Boolean = {
    val javaBinPath = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_JVM_JAVA_BIN_PATH, "java")
    val classpath = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH)
    val mainclass = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS)
    val mainclassArgs = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS_ARGS, StringConstants.EMPTY)
    val jvmOptions = conf.getString(DeployConfKeys.CONFKEY_DEPLOY_JVM_OPTIONS, StringConstants.EMPTY)
    val sessionId = conf.getString(SessionConfKeys.CONFKEY_SESSION_ID)

    if (StringUtils.isBlank(sessionId)) {
      throw new IllegalArgumentException("session Id is blank when creating processor")
    }

    val startCmd = s"${javaBinPath} -cp ${classpath} ${jvmOptions} ${mainclass} ${mainclassArgs} -c . -s ${sessionId} &"
    println(s"${startCmd}")
    commands.add("which java")
    commands.add(startCmd)
    val thread = new Thread(() => {
      val processorBuilder: ProcessBuilder = new ProcessBuilder("/bin/bash", "-c", String.join(StringConstants.SEMICOLON, commands))

      //val process = Runtime.getRuntime.exec(s"${javaBinPath} -cp ${classpath} ${jvmOptions} ${mainclass} ${mainclassArgs} -s ${sessionId} &")

      // todo: 1. redirect output / error stream; 2. add session info; 3. add node manager
      val builderEnv = processorBuilder.environment()

      processorBuilder.redirectOutput(new File("/tmp/jvm/std.out"))
      processorBuilder.redirectError(new File("/tmp/jvm/std.err"))
      val process = processorBuilder.start()

      process.waitFor(1, TimeUnit.SECONDS)
    })

    thread.start()
    thread.join(1)
    println("ready to return")
    thread.isAlive
  }

  def stop(port: Int): Boolean = {
    val processBuilder = new ProcessBuilder("/bin/bash", "-c", s"lsof -t -i:${port} | xargs kill")

    val process = processBuilder.start()

    process.waitFor(1, TimeUnit.SECONDS)
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
