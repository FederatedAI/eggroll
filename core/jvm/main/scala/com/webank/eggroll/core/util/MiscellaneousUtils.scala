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

import org.apache.commons.cli.{CommandLine, CommandLineParser, DefaultParser, HelpFormatter, Option, Options, ParseException}

object MiscellaneousUtils {
  def parseArgs(args: Array[String]): CommandLine = {
    val formatter = new HelpFormatter

    val options = new Options
    val config = Option.builder("c")
      .argName("configuration file")
      .longOpt("config")
      .hasArg.numberOfArgs(1)
      .required
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
      .required
      .desc("session id")
      .build

    val port = Option.builder("p")
      .argName("port to bind")
      .longOpt("port")
      .optionalArg(true)
      .hasArg.numberOfArgs(1)
      .desc("port to bind")
      .build

    options
      .addOption(config)
      .addOption(help)
      .addOption(sessionId)
      .addOption(port)

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
        formatter.printHelp("", options, true)
    }

    cmd
  }
}
