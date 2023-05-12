package com.webank.eggroll.core.containers.container

import com.webank.eggroll.core.util.Logging;

trait ContainerTrait extends Logging {

  def getPid(): Int

  def getContainerId(): String

  def getProcessorId(): Long

  def start(): Boolean

  def stop(): Boolean

  def kill(): Boolean

  def waitForCompletion(): Int
}
