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

package com.webank.eggroll.core.util

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, Options, ParseException}
import org.apache.commons.lang3.StringUtils

object CommandArgsUtils {
  def parseArgs(args: Array[String]): CommandLine = {
    val formatter = new HelpFormatter

    val options = new Options
    val config = Option.builder("c")
      .argName("configuration file")
      .longOpt("config")
      .hasArg.numberOfArgs(1)
//      .required
      .desc("configuration file")
      .build

    val help = Option.builder("h")
      .argName("help")
      .longOpt("help")
      .desc("print this message")
      .build

    val sessionId = Option.builder("s")
      .argName("session id")
      .longOpt("session-id")
      .hasArg.numberOfArgs(1)
//      .required
      .desc("session id")
      .build

    val port = Option.builder("p")
      .argName("port to bind")
      .longOpt("port")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("port to bind")
      .build

    val transferPort = Option.builder("tp")
      .argName("transfer port to bind")
      .longOpt("transfer-port")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("transfer port to bind")
      .build

    val clusterManager = Option.builder("cm")
      .argName("cluster manager of this service")
      .longOpt("cluster-manager")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("cluster manager of this service")
      .build()

    val nodeManager = Option.builder("nm")
        .argName("node manager of this service")
        .longOpt("node-manager")
        .optionalArg(true)
        .hasArg.numberOfArgs(1)
        .desc("node manager of this service")
        .build()

    val serverNodeId = Option.builder("sn")
      .argName("server node id")
      .longOpt("server-node-id")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("server node of this service")
      .build()

    val processorId = Option.builder("prid")
      .argName("processor id")
      .longOpt("processor-id")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("processor id of this service")
      .build()

    options
      .addOption(config)
      .addOption(help)
      .addOption(sessionId)
      .addOption(port)
      .addOption(transferPort)
      .addOption(clusterManager)
      .addOption(nodeManager)
      .addOption(serverNodeId)
      .addOption(processorId)

    val parser = new DefaultParser
    var cmd: CommandLine = null
    try {
      cmd = parser.parse(options, args)
      if (cmd.hasOption("h")) {
        formatter.printHelp("", options, true)
        return null
      }
    } catch {
      case e: ParseException =>
        println(e)
        formatter.printHelp("", options, true)
    }

    cmd
  }
}

object IdUtils {
  private val job = "job"
  private val task = "task"
  def generateJobId(sessionId: String, tag: String = "", delim: String = "-"): String = {
    val result = String.join(delim, sessionId, "scala", job, TimeUtils.getNowMs())
    if (StringUtils.isBlank(tag)) result else s"${result}_${tag}"
  }

  def generateTaskId(jobId: String, partitionId: Int, tag: String = "", delim: String = "-"): String =
    if (StringUtils.isBlank(tag)) String.join(delim, jobId, task, partitionId.toString)
    else String.join(delim, jobId, tag, task, partitionId.toString)
}
