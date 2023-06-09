package com.webank.eggroll.core.containers.meta

import com.webank.eggroll.core.meta.Containers

class ContentType extends Enumeration {
  type ContentType = Value
  val ALL, MODELS, LOGS = Value

}

object ContentType extends ContentType {
  implicit def toProto(t: ContentType.ContentType): Containers.ContentType = {
    t match {
      case ALL => Containers.ContentType.ALL
      case MODELS => Containers.ContentType.MODELS
      case LOGS => Containers.ContentType.LOGS
    }
  }

  implicit def fromProto(proto: Containers.ContentType): ContentType = {
    proto match {
      case Containers.ContentType.ALL => ALL
      case Containers.ContentType.MODELS => MODELS
      case Containers.ContentType.LOGS => LOGS
    }
  }
}
