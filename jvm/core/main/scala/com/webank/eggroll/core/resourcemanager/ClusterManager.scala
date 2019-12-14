package com.webank.eggroll.core.resourcemanager

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.constant.{MetadataCommands, StoreTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErStore, ErStoreLocator}

import scala.reflect.ClassTag

trait ClusterManager {
  def hello(v: ErEndpoint): ErEndpoint
}
object ClusterManager {
  def main(args: Array[String]): Unit = {
    val cc = new CommandClient(ErEndpoint("localhost:4670"))
    val cm = cc.proxy[ClusterManager]
    println(cm.hello(ErEndpoint("hi:80")))
  }
}
class ClusterManagerService extends ClusterManager {
  def hello(v: ErEndpoint): ErEndpoint = v.copy(host = "hello")
}

