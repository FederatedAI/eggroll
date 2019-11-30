package com.webank.eggroll.core.clustermanager

import java.util

import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor}
import com.webank.eggroll.core.nodemanager.{NodeManager, ProcessorManager}
import com.webank.eggroll.core.util.MiscellaneousUtils
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
    ProcessorManager.registerProcessor(ErProcessor(name = "client",
      options = Map(SessionConfKeys.CONFKEY_SESSION_ID -> "0").asJava,
      commandEndpoint = ErEndpoint(host = "localhost", port = clientCommandPort),
      dataEndpoint = ErEndpoint(host = "localhost", port = clientTransferPort)))

    val server = ClusterManager.buildServer(args) // TODO: move to command & transfer

    print("eggroll-standalone-command-port:" + server.getPort)
    print("eggroll-standalone-transfer-port:" + server.getPort)
    server.awaitTermination() // 返回 port, 传入单机python进程Id
  }
}
