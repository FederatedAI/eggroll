package com.webank.eggroll.core.resourcemanager.job.container

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class ContainersManager(ec: ExecutionContext,
                        callbacks: Seq[ContainerStatusCallback] = Seq.empty) {

  private val handlers = TrieMap[Long, ContainerLifecycleHandler]()
  private val executor = Executors.newSingleThreadScheduledExecutor()
  executor.scheduleAtFixedRate(
    () => handlers.retain((_, handler) => !handler.isCompleted), 0, 1, TimeUnit.MINUTES)

  def addContainer(containerId: Long, container: ContainerTrait): Unit = {
    val lifecycleHandler = ContainerLifecycleHandler
      .builder()
      .withContainer(container)
      .withCallbacks(callbacks)
      .build()(ec)
    handlers.putIfAbsent(containerId, lifecycleHandler)
  }

  def startContainer(containerId: Long): Unit = {
    handlers.get(containerId).foreach(_.startContainer())
  }

  def stopContainer(containerId: Long): Unit = {
    handlers.get(containerId).foreach(_.stopContainer())
  }

  def killContainer(containerId: Long): Unit = {
    handlers.get(containerId).foreach(_.killContainer())
  }

  def stop(): Unit = {
    executor.shutdown()
  }

}

object ContainersManager {
  def builder(): ContainersManagerBuilder = {
    new ContainersManagerBuilder()
  }
}

class ContainersManagerBuilder extends ContainerStatusCallbacksBuilder {
  def build()(implicit ec: ExecutionContext): ContainersManager = {
    new ContainersManager(ec, callbacks)
  }
}