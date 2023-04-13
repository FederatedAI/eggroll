package com.webank.eggroll.core.resourcemanager.job.container

import com.webank.eggroll.core.util.Logging

trait ContainerTrait extends Logging {
  def start(): Boolean

  def stop(): Boolean

  def kill(): Boolean

  def waitForCompletion(): Int
}