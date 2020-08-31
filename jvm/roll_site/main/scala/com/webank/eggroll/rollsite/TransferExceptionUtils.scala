package com.webank.eggroll.rollsite

import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Topic
import com.webank.eggroll.core.util.RuntimeUtils
import io.grpc.{Status, StatusRuntimeException}
import org.apache.commons.lang3.exception.ExceptionUtils


object TransferExceptionUtils {

  private def genExceptionDescription(t: Throwable, topic: Topic = null): String = {
    val locMsg = t.getLocalizedMessage
    val stackInfo = ExceptionUtils.getStackTrace(t)
    var desc = ""
    val host = RuntimeUtils.getMySiteLocalAddressAndPort()
    if (locMsg.contains("[Roll Site Error TransInfo]")) {
      desc = locMsg + f"--> $host"
    } else {
      desc = f"\n[Roll Site Error TransInfo] \n location msg:$locMsg \n stack info: $stackInfo \n"
      if (topic != null) {
        val locationInfo = f"\nlocationInfo: topic.getName--${topic.getName} " +
          f"topic.getPartyId--${topic.getPartyId}"
        desc = desc + locationInfo
      }
      desc = desc + f"\nexception trans path: $host"
    }
    desc
  }

  def throwableToException(t: Throwable, topic: Topic = null): StatusRuntimeException = {
    if (t.isInstanceOf[StatusRuntimeException]) return t.asInstanceOf[StatusRuntimeException]
    val desc = genExceptionDescription(t, topic)
    val status = Status.fromThrowable(t).withDescription(desc)
    status.asRuntimeException()
  }

  def checkPacketIsException(request: Proxy.Packet): Boolean = {
    val key = request.getBody.getKey
    //println("checkPacketIsException", key)
    if (key.contains("[roll site transfer exception]") ) {
      return true
    }
    false
  }

  def genExceptionToNextSite(request: Proxy.Packet, t: Throwable = null): Proxy.Packet = {
    // good data
    if (!checkPacketIsException(request) && t == null) return request

    // gen exception info
    var decsException = ""
    if (t != null) {
      decsException = genExceptionDescription(t, request.getHeader.getDst)
    }

    // if packet is an exception from pre site, then get it error info
    var nextData = if (checkPacketIsException(request)) {
      new String(request.getBody.getValue.toByteArray)
    } else {
      ""
    }

    // merge all exceptions data
    if (checkPacketIsException(request)) {
      val host = RuntimeUtils.getMySiteLocalAddressAndPort()
      nextData = nextData + f"--> $host" + "\n ==exception divider== \n" + decsException
    } else {
      nextData = decsException
    }

    // gen a new packet to next site
    Proxy.Packet.newBuilder()
      .setHeader(request.getHeader)
      .setBody(Proxy.Data.newBuilder()
        .setKey("[roll site transfer exception]")
        .setValue(ByteString.copyFromUtf8(nextData)))
      .build()
  }
}
