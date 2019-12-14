package com.webank.eggroll.core.resourcemanager

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.constant.{MetadataCommands, StoreTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErStore, ErStoreLocator}

import scala.reflect.ClassTag

trait StoreManager {
  def hello(v: ErEndpoint): ErEndpoint
}
object StoreManager {
  def main(args: Array[String]): Unit = {
    val cc = new CommandClient(ErEndpoint("localhost:4670"))
    val cm = cc.proxy[StoreManager]
    println(cm.hello(ErEndpoint("hi:80")))
  }
}
class StoreManagerService extends StoreManager {
  def hello(v: ErEndpoint): ErEndpoint = v.copy(host = "hello")
}

