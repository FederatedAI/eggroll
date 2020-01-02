package com.webank.eggroll.rollsite

import com.webank.eggroll.core.Bootstrap

class EggSiteBootstrap extends Bootstrap {
  private var args: Array[String] = _
  override def init(args: Array[String]): Unit = this.args = args

  override def start(): Unit = {
    Proxy.main(args)
  }
}
