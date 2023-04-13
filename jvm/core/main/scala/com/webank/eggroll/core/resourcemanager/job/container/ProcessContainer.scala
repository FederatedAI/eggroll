package com.webank.eggroll.core.resourcemanager.job.container

import java.io._

class ProcessContainer(
                        command: Seq[String],
                        extraEnv: Map[String, String] = Map.empty,
                        stdOutFile: Option[File] = None,
                        stdErrFile: Option[File] = None,
                        cwd: Option[File] = None) extends ContainerTrait {

  private var process: java.lang.Process = _

  // TODO: add container preparation logic such as downloading files

  def start(): Boolean = {
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
