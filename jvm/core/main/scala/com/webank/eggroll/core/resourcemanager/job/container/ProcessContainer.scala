package com.webank.eggroll.core.resourcemanager.job.container

import java.io._
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

  def start(): Boolean = {
    workingDirectoryPreparer.foreach(_.prepare())
    try {
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
