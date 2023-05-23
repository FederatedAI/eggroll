package com.webank.eggroll.core.containers

import com.webank.eggroll.core.session.{ErConf, StaticErConf};

package object container {
  object ContainerKey {
    val WORKING_DIR = "eggroll.container.dir"
    val PYTHON_EXEC = "eggroll.container.python.exec"
    val LOGS_DIR = "eggroll.container.logs"

    val DEEPSPEED_PYTHON_EXEC = "eggroll.container.deepspeed.python.exec"
    val DEEPSPEED_SCRIPT_PATH = "eggroll.container.deepspeed.script.path"
    val DEEPSPEED_TORCH_DISTRIBUTED_BACKEND = "eggroll.container.deepspeed.distributed.backend"
    val DEEPSPEED_TORCH_DISTRIBUTED_STORE_HOST = "eggroll.container.deepspeed.distributed.store.host"
    val DEEPSPEED_TORCH_DISTRIBUTED_STORE_PORT = "eggroll.container.deepspeed.distributed.store.port"
  }

  type ContainerStatusCallback = (ContainerStatus.Value) => (ContainerTrait, Option[Exception]) => Unit

  class PythonContainerRuntimeConfig(options: Map[String, String] = Map()) extends ErConf {

    StaticErConf.getAll.foreach { case (key, value) =>
      this.conf.put(key, value)
    }

    options.foreach { case (key, value) =>
      this.conf.put(key, value)
    }

    override def getModuleName(): String = "PythonContainerRuntimeConfig"

    override def getPort(): Int = ???

    def getPythonExec(key: String = ""): String = {
      val fallbacks = Seq(
        () => Option(key).filter(_.nonEmpty).map(k => getString(k)).getOrElse(""),
        () => getString(ContainerKey.PYTHON_EXEC)
      )
      for (f <- fallbacks) {
        val value = f()
        if (value.nonEmpty) {
          return value
        }
      }
      throw new RuntimeException(s"python exec not found for key: $key")
    }
  }
}
