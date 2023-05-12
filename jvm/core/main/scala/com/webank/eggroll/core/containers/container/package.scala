package com.webank.eggroll.core.containers

import com.webank.eggroll.core.session.RuntimeErConf;

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

  class PythonContainerRuntimeConfig(runtimeErConf: RuntimeErConf) extends RuntimeErConf {

    runtimeErConf.getAll.foreach { case (key, value) =>
      this.conf.put(key, value)
    }

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
