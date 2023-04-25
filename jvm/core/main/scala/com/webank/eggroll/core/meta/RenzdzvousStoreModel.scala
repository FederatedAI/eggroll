package com.webank.eggroll.core.meta

import com.webank.eggroll.core.serdes.{BaseSerializable, PbMessageDeserializer, PbMessageSerializer}
import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.datastructure.RpcMessage

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

trait RendezvousStoreRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "RendezvousStore"
}

case class RendezvousStoreSetRequest(prefix: String, key: K, value: V) extends RendezvousStoreRpcMessage

case class RendezvousStoreSetResponse() extends RendezvousStoreRpcMessage

case class RendezvousStoreGetRequest(prefix: String, key: K, timeout: FiniteDuration) extends RendezvousStoreRpcMessage

case class RendezvousStoreGetResponse(value: V) extends RendezvousStoreRpcMessage

case class RendezvousStoreAddRequest(prefix: String, key: K, amount: Long) extends RendezvousStoreRpcMessage

case class RendezvousStoreAddResponse(amount: Long) extends RendezvousStoreRpcMessage


object RendezvousStoreModelPbMessageSerdes {
  implicit class RendezvousStoreSetRequestToPbMessage(src: RendezvousStoreSetRequest) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.SetRequest = {
      val builder = Store.SetRequest.newBuilder()
        .setPrefix(src.prefix)
        .setKey(ByteString.copyFrom(src.key.toArray))
        .setValue(ByteString.copyFrom(src.value.toArray))
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreSetRequest].toBytes()
  }


  implicit class RendezvousStoreSetRequestFromPbMessage(src: Store.SetRequest) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreSetRequest = {
      RendezvousStoreSetRequest(
        prefix = src.getPrefix,
        key = src.getKey.toByteArray.toVector,
        value = src.getValue.toByteArray.toVector
      )
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreSetRequest = {
      Store.SetRequest.parseFrom(bytes).fromProto()
    }
  }

  implicit class RendezvousStoreSetResponseToPbMessage(src: RendezvousStoreSetResponse) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.SetRequest = {
      val builder = Store.SetRequest.newBuilder()
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreSetResponse].toBytes()
  }

  implicit class RendezvousStoreSetResponseFromPbMessage(src: Store.SetResponse) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreSetResponse = {
      RendezvousStoreSetResponse()
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreSetResponse = {
      Store.SetResponse.parseFrom(bytes).fromProto()
    }
  }

  implicit class RendezvousStoreGetRequestToPbMessage(src: RendezvousStoreGetRequest) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.GetRequest = {
      val builder = Store.GetRequest.newBuilder()
        .setPrefix(src.prefix)
        .setKey(ByteString.copyFrom(src.key.toArray))
        .setTimeout(com.google.protobuf.Duration.newBuilder()
          .setSeconds(src.timeout.toSeconds)
          .setNanos(src.timeout.toNanos.toInt % 1000000000)
          .build())
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreGetRequest].toBytes()
  }

  implicit class RendezvousStoreGetRequestFromPbMessage(src: Store.GetRequest) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreGetRequest = {
      RendezvousStoreGetRequest(
        prefix = src.getPrefix,
        key = src.getKey.toByteArray.toVector,
        timeout = FiniteDuration(src.getTimeout.getSeconds * 1000000000L + src.getTimeout.getNanos, NANOSECONDS)
      )
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreGetRequest = {
      Store.GetRequest.parseFrom(bytes).fromProto()
    }
  }

  implicit class RendezvousStoreGetResponseToPbMessage(src: RendezvousStoreGetResponse) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.GetResponse = {
      val builder = Store.GetResponse.newBuilder()
        .setValue(ByteString.copyFrom(src.value.toArray))
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreGetResponse].toBytes()
  }

  implicit class RendezvousStoreGetResponseFromPbMessage(src: Store.GetResponse) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreGetResponse = {
      RendezvousStoreGetResponse(
        value = src.getValue.toByteArray.toVector
      )
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreGetResponse = {
      Store.GetResponse.parseFrom(bytes).fromProto()
    }
  }

  implicit class RendezvousStoreAddRequestToPbMessage(src: RendezvousStoreAddRequest) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.AddRequest = {
      val builder = Store.AddRequest.newBuilder()
        .setPrefix(src.prefix)
        .setKey(ByteString.copyFrom(src.key.toArray))
        .setAmount(src.amount)
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreAddRequest].toBytes()
  }

  implicit class RendezvousStoreAddRequestFromPbMessage(src: Store.AddRequest) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreAddRequest = {
      RendezvousStoreAddRequest(
        prefix = src.getPrefix,
        key = src.getKey.toByteArray.toVector,
        amount = src.getAmount
      )
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreAddRequest = {
      Store.AddRequest.parseFrom(bytes).fromProto()
    }
  }

  implicit class RendezvousStoreAddResponseToPbMessage(src: RendezvousStoreAddResponse) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Store.AddResponse = {
      val builder = Store.AddResponse.newBuilder()
        .setAmount(src.amount)
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[RendezvousStoreAddResponse].toBytes()
  }

  implicit class RendezvousStoreAddResponseFromPbMessage(src: Store.AddResponse) extends PbMessageDeserializer {
    override def fromProto[T >: RendezvousStoreRpcMessage](): RendezvousStoreAddResponse = {
      RendezvousStoreAddResponse(src.getAmount)
    }

    override def fromBytes(bytes: Array[Byte]): RendezvousStoreAddResponse = {
      Store.AddResponse.parseFrom(bytes).fromProto()
    }
  }
}