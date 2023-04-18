package com.webank.eggroll.core.resourcemanager.job.container

import java.io.{ByteArrayInputStream, FileOutputStream, InputStream}
import java.nio.file.Path
import java.util.zip.{ZipEntry, ZipInputStream}

class WorkingDirectoryPreparer(
                                var files: Map[String, Array[Byte]],
                                var zippedFiles: Map[String, Array[Byte]],
                                var need_cleanup: Boolean = true,
                                var workingDir: Path = null
                              ) {

  def setWorkingDir(workingDir: Path): Unit = {
    this.workingDir = workingDir
  }

  def setNeedCleanup(need_cleanup: Boolean): Unit = {
    this.need_cleanup = need_cleanup
  }

  private def getWorkingDir(): Path = {
    if (workingDir != null) {
      workingDir
    } else {
      throw new Exception("workingDir is not set")
    }
  }

  def prepare(): Unit = {
    getWorkingDir().toFile.mkdirs()
    files.foreach { case (fileName, content) =>
      val file = getWorkingDir().resolve(fileName).toFile
      val fos = new FileOutputStream(file)
      fos.write(content)
      fos.close()
    }
    zippedFiles.foreach { case (dirname, zipBytes) =>
      val zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipBytes))
      val buffer = new Array[Byte](1024)

      def extractEntry(entry: ZipEntry, inputStream: InputStream, outputPath: Path): Unit = {
        val file = outputPath.toFile
        file.getParentFile.mkdirs()
        val fileOutputStream = new FileOutputStream(file)

        try {
          var bytesRead = inputStream.read(buffer)
          while (bytesRead != -1) {
            fileOutputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
        } finally {
          fileOutputStream.close()
        }
      }

      Iterator.continually(zipInputStream.getNextEntry)
        .takeWhile(_ != null)
        .foreach(entry => {
          val outputPath = getWorkingDir().resolve(dirname).resolve(entry.getName)
          if (!entry.isDirectory) {
            extractEntry(entry, zipInputStream, outputPath)
          }
          zipInputStream.closeEntry()
        })

      zipInputStream.close()
    }
    // clear files and zippedFiles
    files = Map.empty
    zippedFiles = Map.empty
  }

  def cleanup(): Unit = {
    if (need_cleanup && workingDir != null) {
      workingDir.toFile.delete()
    }
  }
}
