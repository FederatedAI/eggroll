package com.webank.eggroll.core.containers

object JobProcessorTypes extends Enumeration {
  val DeepSpeed = Value

  def fromString(color: String): Option[JobProcessorTypes.Value] = {
    color.toLowerCase match {
      case "deepspeed" => Some(DeepSpeed)
      case _ => None
    }
  }
}
