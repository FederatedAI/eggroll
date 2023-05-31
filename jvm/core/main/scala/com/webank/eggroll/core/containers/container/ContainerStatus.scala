package com.webank.eggroll.core.containers.container;

object ContainerStatus extends Enumeration {
  val Pending, Started, Failed, Poison, Success, Exception = Value
}
