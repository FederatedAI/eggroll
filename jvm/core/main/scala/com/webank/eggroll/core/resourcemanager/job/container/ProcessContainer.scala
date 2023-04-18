package com.webank.eggroll.core.resourcemanager.job.container

import java.io._

class ProcessContainer(
                        command: Seq[String],
                        extraEnv: Map[String, String] = Map.empty,
                        stdOutFile: Option[File] = None,
                        stdErrFile: Option[File] = None,
                        cwd: Option[File] = None,
                        workingDirectoryPreparer: Option[WorkingDirectoryPreparer] = None
                      ) extends ContainerTrait {

  private var process: java.lang.Process = _

  // set working dir for workingDirectoryPreparer
  workingDirectoryPreparer.foreach(wdp => cwd.foreach(f => wdp.setWorkingDir(f.toPath)))

  def start(): Boolean = {
    workingDirectoryPreparer.foreach(_.prepare())
    try {
      val javaProcessBuilder = new java.lang.ProcessBuilder(command: _*)
      stdOutFile.foreach { f =>
        f.getParentFile.mkdirs()
        javaProcessBuilder.redirectOutput(f)
      }
      stdErrFile.foreach { f =>
        f.getParentFile.mkdirs()
        javaProcessBuilder.redirectError(f)
      }
      val environment = javaProcessBuilder.environment()
      extraEnv.foreach { case (k, v) =>
        environment.put(k, v)
      }
      cwd.foreach { f =>
        javaProcessBuilder.directory(f)
      }
      process = javaProcessBuilder.start()
      process.isAlive
    } finally {
      workingDirectoryPreparer.foreach(_.cleanup())
    }
  }

  def waitForCompletion(): Int = {
    process.waitFor()
    process.exitValue()
  }

  def stop(): Boolean = {
    process.destroy()
    process.isAlive
  }

  def kill(): Boolean = {
    process.destroyForcibly()
    process.isAlive
  }
}
