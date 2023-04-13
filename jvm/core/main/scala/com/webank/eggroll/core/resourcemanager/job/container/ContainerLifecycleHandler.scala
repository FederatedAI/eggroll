package com.webank.eggroll.core.resourcemanager.job.container

import com.webank.eggroll.core.util.Logging

import scala.concurrent.{ExecutionContext, Future}


class ContainerLifecycleHandler(container: ContainerTrait,
                                callbacks: Seq[ContainerStatusCallback] = Seq.empty,
                                ec: ExecutionContext) extends Logging {

  private var watcher: Future[Unit] = _
  private var status: ContainerStatus.Value = _
  setStatus(ContainerStatus.Pending)

  def isCompleted: Boolean = {
    status match {
      case ContainerStatus.Failed => true
      case ContainerStatus.Success => true
      case ContainerStatus.Exception => true
      case _ => false
    }
  }

  def isSuccess: Boolean = {
    status match {
      case ContainerStatus.Success => true
      case _ => false
    }
  }

  def isFailed: Boolean = {
    status match {
      case ContainerStatus.Failed => true
      case _ => false
    }
  }

  def isStarted: Boolean = {
    status match {
      case ContainerStatus.Started => true
      case _ => false
    }
  }

  private def setStatus(status: ContainerStatus.Value, e: Option[Exception] = None): Unit = {
    if (!isCompleted && this.status != status) {
      this.status = status
      logInfo(s"Container ${container} status changed to $status")
      if (e.isDefined) {
        logError(s"Container ${container} failed", e.get)
      }
      applyCallbacks(status, e)
    }
  }

  def startContainer(): Unit = {
    if (isStarted) {
      throw new IllegalStateException("Container already started")
    }
    if (isCompleted) {
      throw new IllegalStateException("Container already completed")
    }
    setStatus(ContainerStatus.Started)
    watcher = Future {
      try {
        container.start()
        val exitCode = container.waitForCompletion()
        if (exitCode != 0) {
          setStatus(ContainerStatus.Failed)
        } else {
          setStatus(ContainerStatus.Success)
        }
      } catch {
        case e: InterruptedException =>
          setStatus(ContainerStatus.Exception, Some(e))
      }
    }(ec)
  }

  def stopContainer(): Boolean = {
    if (isStarted) {
      container.stop()
    } else {
      logError(s"Container ${container} not started, cannot stop")
      false
    }
  }

  def killContainer(): Boolean = {
    if (isStarted) {
      container.kill()
    } else {
      logError(s"Container ${container} not started, cannot kill")
      false
    }
  }

  private def applyCallbacks(status: ContainerStatus.Value, e: Option[Exception]): Unit = {
    callbacks.foreach(_.apply(status)(container, e))
  }

}

object ContainerLifecycleHandler {
  def builder(): ContainerLifecycleHandlerBuilder = {
    new ContainerLifecycleHandlerBuilder()
  }
}


class ContainerLifecycleHandlerBuilder extends ContainerStatusCallbacksBuilder {
  private var container: ContainerTrait = _

  def withContainer(container: ContainerTrait): ContainerLifecycleHandlerBuilder = {
    this.container = container
    this
  }

  def build()(implicit ec: ExecutionContext): ContainerLifecycleHandler = {
    new ContainerLifecycleHandler(container, callbacks, ec)
  }
}

class ContainerStatusCallbacksBuilder {
  var callbacks: Seq[ContainerStatusCallback] = Seq.empty

  def withCallback(status: ContainerStatus.Value, callback: (ContainerTrait, Option[Exception]) => Unit): this.type = {
    val _callback: ContainerStatus.Value => (ContainerTrait, Option[Exception]) => Unit =
      (s: ContainerStatus.Value) => (c: ContainerTrait, e: Option[Exception]) => {
        if (s == status) {
          callback(c, e)
        }
      }
    this.callbacks = this.callbacks :+ _callback
    this
  }

  def withCallbacks(callbacks: Seq[ContainerStatus.Value => (ContainerTrait, Option[Exception]) => Unit]): this.type = {
    this.callbacks = this.callbacks ++ callbacks
    this
  }

  def withPendingCallback(callback: ContainerTrait => Unit): this.type = {
    withCallback(ContainerStatus.Pending, (c: ContainerTrait, _: Option[Exception]) => callback(c))
  }

  def withStartedCallback(callback: ContainerTrait => Unit): this.type = {
    withCallback(ContainerStatus.Started, (c: ContainerTrait, _: Option[Exception]) => callback(c))
  }

  def withFailedCallback(callback: ContainerTrait => Unit): this.type = {
    withCallback(ContainerStatus.Failed, (c: ContainerTrait, _: Option[Exception]) => callback(c))
  }

  def withSuccessCallback(callback: ContainerTrait => Unit): this.type = {
    withCallback(ContainerStatus.Success, (c: ContainerTrait, _: Option[Exception]) => callback(c))
  }

  def withExceptionCallback(callback: (ContainerTrait, Option[Exception]) => Unit): this.type = {
    withCallback(ContainerStatus.Exception, callback)
  }
}