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
import io.grpc.ServerInterceptors
import org.apache.commons.lang3.StringUtils


class EggSiteBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var securePort = 0
  private var confPath = ""
  // todo:0: configurable

  private var pollingThreadPool: ThreadPoolExecutor = _

  override def init(args: Array[String]): Unit = {
    val cmd = CommandArgsUtils.parseArgs(args = args)
    this.confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', RollSiteConfKeys.EGGROLL_ROLLSITE_PORT.get()).toInt
    this.securePort = cmd.getOptionValue("sp", RollSiteConfKeys.EGGROLL_ROLLSITE_SECURE_PORT.get()).toInt
    val routerFilePath = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_PATH.get()
    logInfo(s"init router. path: $routerFilePath")
    Router.initOrUpdateRouterTable(routerFilePath)
    WhiteList.init()

    if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_CLIENT_ENABLED.get().toBoolean) {
      logInfo(s"start pooling client")
      val pollingPushConcurrency = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_CONCURRENCY.get().toInt
      if (pollingPushConcurrency > 0) {
        pollingThreadPool = ThreadPoolUtils.newFixedThreadPool(pollingPushConcurrency, "polling-client")
        for (i <- 0 until pollingPushConcurrency) {
          pollingThreadPool.execute(() => {
            val dataTransferClient = new LongPollingClient
            dataTransferClient.pollingForever()
          })
        }
      }
    }
  }

  override def start(): Unit = {
    var option: Map[String, String] = Map({"eggroll.core.security.secure.cluster.enabled" ->"false"})
    val tranferServicer = new EggSiteServicer
    val ipGetService = ServerInterceptors.intercept(tranferServicer.bindService, new AddrAuthServerInterceptor)
    val plainServer = GrpcServerUtils.createServer(
      port = this.port,
      grpcServices = List(tranferServicer),
      bindServices = List(ipGetService),
      options = option)
    plainServer.start()
    this.port = plainServer.getPort

    val msg = s"server started at $port"
    logInfo(msg)

    val serverCrt = CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_CRT_PATH.get()
    val serverKey = CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_PATH.get()
    if (!StringUtils.isBlank(serverCrt) && !StringUtils.isBlank(serverKey)) {
      option = Map({"eggroll.core.security.secure.cluster.enabled" -> "true"})
      val tranferServicerSecure = new EggSiteServicer
      val ipGetServiceSecure = ServerInterceptors.intercept(tranferServicerSecure.bindService, new AddrAuthServerInterceptor)
      val plainServerSecure = GrpcServerUtils.createServer(
        port = this.securePort,
        grpcServices = List(tranferServicerSecure),
        bindServices = List(ipGetServiceSecure),
        options = option)
      plainServerSecure.start()
      this.securePort = plainServerSecure.getPort

      val msg2 = s"secure server started at $securePort"
      logInfo(msg2)
    }
  }
}

object EggSiteBootstrap {
  def main(args: Array[String]): Unit = {
    val rsBootstrap = new EggSiteBootstrap()
    rsBootstrap.init(args)
    rsBootstrap.start()
  }
}
