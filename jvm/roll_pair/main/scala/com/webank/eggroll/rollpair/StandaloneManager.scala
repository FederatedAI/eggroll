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

package com.webank.eggroll.rollpair

import com.webank.eggroll.core.clustermanager.ClusterManager
import com.webank.eggroll.core.constant.NodeManagerConfKeys
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.nodemanager.NodeManager
import com.webank.eggroll.core.session.StaticErConf
import org.apache.commons.cli.{DefaultParser, Options}
object StandaloneManager {
  // usage: -ccp 4677 -ctp 4677
  def main(args: Array[String]): Unit = {
    NodeManager.registerRouter()
    Main.registerRouter()
    val server = ClusterManager.buildServer(args)
    val options = new Options().addOption("s",true,"session id")
      .addOption("c","ignore").addOption("p","ignore")
    val sid = new DefaultParser().parse(options, args).getOptionValue("s")
    println("eggroll-standalone-manager-port:" + server.getPort)
    Main.reportCM(sid, ErEndpoint("localhost", server.getPort), server.getPort)
    server.awaitTermination() // returns port, pass standalone python process id
  }
}
