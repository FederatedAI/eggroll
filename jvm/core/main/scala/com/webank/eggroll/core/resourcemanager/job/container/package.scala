package com.webank.eggroll.core.resourcemanager.job

package object container {
  type ContainerStatusCallback = (ContainerStatus.Value) => (ContainerTrait, Option[Exception]) => Unit
}
