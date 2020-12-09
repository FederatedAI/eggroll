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
import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadPoolExecutor, TimeUnit}

import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.constant.{CoreConfKeys, RollSiteConfKeys}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging, RuntimeMetricUtils, ThreadPoolUtils}
import io.grpc.ServerInterceptors
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.concurrent.BasicThreadFactory


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
    logInfo(s"initing router at path=${routerFilePath}")
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

    logInfo(s"start refreshing route table per min")
    val routineExecutors = new ScheduledThreadPoolExecutor(5,
      new BasicThreadFactory.Builder().namingPattern("routine-executor-%d").build)
    routineExecutors.scheduleAtFixedRate(() =>
      Router.initOrUpdateRouterTable(routerFilePath), 1, 1, TimeUnit.MINUTES)

    val isDirectMemoryMetricStatsEnabled = CoreConfKeys.EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS.get().toBoolean
    if (isDirectMemoryMetricStatsEnabled) {
      logDebug(s"${RuntimeMetricUtils.getDefaultArenaInfo()}")
      val interval = CoreConfKeys.EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS_INTERVAL_SEC.get().toLong
      routineExecutors.scheduleAtFixedRate(() => {
        logDebug(s"metrics - \n[METRICS] totalDirectMemorySize=${RuntimeMetricUtils.getDirectMemorySize().get()}, \n" +
          s"[METRICS] GrpcByteBufAllocatorMetrics=${RuntimeMetricUtils.getGrpcByteBufAllocatorMetrics()}, \n" +
          s"[METRICS] DefaultDirectUnpooledArenaMetrics=${RuntimeMetricUtils.getDefaultDirectUnpooledArenaMetrics()}")
      }, 0, interval, TimeUnit.SECONDS)
    }
  }

  override def start(): Unit = {
    var option: Map[String, String] = Map({"eggroll.core.security.secure.cluster.enabled" -> "false"})
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

