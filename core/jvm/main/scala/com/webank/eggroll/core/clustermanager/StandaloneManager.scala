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

package com.webank.eggroll.core.clustermanager

import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor}
import com.webank.eggroll.core.nodemanager.{EggManager, NodeManager}
import org.apache.commons.cli.{DefaultParser, Options}

import scala.collection.JavaConverters._
object StandaloneManager {
  // usage: -ccp 4677 -ctp 4677
  def main(args: Array[String]): Unit = {
    ClusterManager.registerRouter()
    NodeManager.registerRouter()
    val parser = new DefaultParser
    val options = new Options().addOption("ccp","client-command-port", true, "client-command-port")
      .addOption("ctp","client-transfer-port", true, "client-transfer-port")
    val cmd = parser.parse(options, args)
    val clientCommandPort = cmd.getOptionValue("client-command-port").toInt
    val clientTransferPort = cmd.getOptionValue("client-transfer-port").toInt
    EggManager.register(ErProcessor(name = "client",
      options = Map(SessionConfKeys.CONFKEY_SESSION_ID -> "0").asJava,
      commandEndpoint = ErEndpoint(host = "localhost", port = clientCommandPort),
      dataEndpoint = ErEndpoint(host = "localhost", port = clientTransferPort)))

    val server = ClusterManager.buildServer(args) // TODO: move to command & transfer

    print("eggroll-standalone-command-port:" + server.getPort)
    print("eggroll-standalone-transfer-port:" + server.getPort)
    server.awaitTermination() // 返回 port, 传入单机python进程Id
  }
}
