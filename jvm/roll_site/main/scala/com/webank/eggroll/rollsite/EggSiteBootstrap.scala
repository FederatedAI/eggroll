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

package com.webank.eggroll.rollsite

import java.io.File
import java.util.concurrent.ThreadPoolExecutor

import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.constant.{CoreConfKeys, RollSiteConfKeys}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging, ThreadPoolUtils}

import scala.collection.immutable.Range

class EggSiteBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var securePort = 0
  private var confPath = ""
  // todo:0: configurable
  private val pollingPushConcurrency = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_PUSH_CONCURRENCY.get().toInt
  private val pollingUnaryCallConcurrency = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_UNARYCALL_CONCURRENCY.get().toInt
  private var pollingPushThreadPool: ThreadPoolExecutor = _
  private var pollingUnaryCallThreadPool: ThreadPoolExecutor = _

  override def init(args: Array[String]): Unit = {
    val cmd = CommandArgsUtils.parseArgs(args = args)
    this.confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', RollSiteConfKeys.EGGROLL_ROLLSITE_PORT.get()).toInt
    val routerFilePath = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_PATH.get()
    logInfo(s"init router. path: $routerFilePath")
    Router.initOrUpdateRouterTable(routerFilePath)

    if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_CLIENT_ENABLED.get().toBoolean) {
      if (pollingPushConcurrency > 0) {
        pollingPushThreadPool = ThreadPoolUtils.newFixedThreadPool(pollingPushConcurrency, "polling-push")
        for (i <- 0 until pollingPushConcurrency) {
          pollingPushThreadPool.execute(() => {
            val dataTransferClient = new LongPollingClient
            dataTransferClient.pollingForever("push")
          })
        }
      }

      if (pollingUnaryCallConcurrency > 0) {
        pollingUnaryCallThreadPool = ThreadPoolUtils.newFixedThreadPool(pollingUnaryCallConcurrency, "polling-unary-call")
        for (i <- 0 until pollingUnaryCallConcurrency) {
          pollingUnaryCallThreadPool.execute(() => {
            val dataTransferClient = new LongPollingClient
            dataTransferClient.pollingForever("unaryCall")
          })
        }
      }
    }
  }

  override def start(): Unit = {
    val plainServer = GrpcServerUtils.createServer(port = this.port, grpcServices = List(new EggSiteServicer))
    plainServer.start()
    this.port = plainServer.getPort

    val msg = s"server started at $port"
    logInfo(msg)
    println(msg)
  }
}

object EggSiteBootstrap {
  def main(args: Array[String]): Unit = {
    val rsBootstrap = new EggSiteBootstrap()
    rsBootstrap.init(args)
    rsBootstrap.start()
  }
}
