package com.webank.eggroll.core.resourcemanager.job.container

object ContainerStatus extends Enumeration {
  val Pending, Started, Failed, Success, Exception = Value
}
