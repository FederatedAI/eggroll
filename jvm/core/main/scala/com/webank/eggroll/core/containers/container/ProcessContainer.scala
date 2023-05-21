package com.webank.eggroll.core.containers.container

import java.nio.file.Path

class ProcessContainer(
                        command: Seq[String],
                        cwd: Path,
                        extraEnv: Map[String, String] = Map.empty,
                        stdOutFile: Option[Path] = None,
                        stdErrFile: Option[Path] = None,
                        workingDirectoryPreparer: Option[WorkingDirectoryPreparer] = None,
                        containerId: String,
                        processorId: Long
                      ) extends ContainerTrait {

  private var process: java.lang.Process = _

  // set working dir for workingDirectoryPreparer
  workingDirectoryPreparer.foreach(wdp => wdp.setWorkingDir(cwd))

  def preStart(): Unit = {}

  def postStart(): Unit = {}

  def start(): Boolean = {
    preStart()
    workingDirectoryPreparer.foreach(_.prepare())
    val output = try {
      println(s"============== command ${command} ")
      val javaProcessBuilder = new java.lang.ProcessBuilder(command: _*)
        .directory(cwd.toFile)
      stdOutFile.foreach { f =>
        val p = cwd.resolve(f).toFile
        p.getParentFile.mkdirs()
        javaProcessBuilder.redirectOutput(p)
      }
      stdErrFile.foreach { f =>
        val p = cwd.resolve(f).toFile
        p.getParentFile.mkdirs()
        javaProcessBuilder.redirectError(p)
      }
      val environment = javaProcessBuilder.environment()
      extraEnv.foreach { case (k, v) =>
        environment.put(k, v)
      }
      process = javaProcessBuilder.start()
      process.isAlive
    } finally {
      workingDirectoryPreparer.foreach(_.cleanup())
    }
    postStart()
    output
  }

  def waitForCompletion(): Int = {
    process.waitFor()
    process.exitValue()
  }

  def stop(): Boolean = {
    if (process.isAlive) {
      process.destroy()
    }
    process.isAlive
  }

  def kill(): Boolean = {
    if (process.isAlive) {
      process.destroyForcibly()
    }
    process.isAlive
  }

  override def toString: String = {
    s"ProcessContainer(command=$command, cwd=$cwd, extraEnv=$extraEnv, stdOutFile=$stdOutFile, stdErrFile=$stdErrFile, workingDirectoryPreparer=$workingDirectoryPreparer)"
  }

  override def getContainerId(): String = {
    containerId
  }

  override def getProcessorId(): Long = {
    processorId
  }

  override def getPid(): Int = {
    val pidField = process.getClass.getDeclaredField("pid")
    pidField.setAccessible(true)
    pidField.getInt(process)
  }

}
