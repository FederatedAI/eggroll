package com.webank.eggroll.rollsite

import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.ai.eggroll.api.networking.proxy.Proxy.{PollingFrame, Topic}
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.util.{ErrorUtils, RuntimeUtils}
import io.grpc.{Status, StatusRuntimeException}
import org.apache.commons.lang3.exception.ExceptionUtils


object TransferExceptionUtils {

  private def genExceptionDescription(t: Throwable, topic: Topic = null): String = {
    val locMsg = t.getLocalizedMessage
    val stackInfo = ExceptionUtils.getStackTrace(t)
    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    var desc = s"Error from partyId=${myPartyId}:\n-------------\n"
    val host = RollSiteConfKeys.EGGROLL_ROLLSITE_HOST.get()
    if (locMsg != null && locMsg.contains("[Roll Site Error TransInfo]")) {
      desc = s"${locMsg} --> ${host}(${myPartyId})"
    } else {
      desc = f"\n[Roll Site Error TransInfo] \n location msg=${locMsg} \n stack info=${stackInfo} \n"
      if (topic != null) {
        val locationInfo = f"\nlocationInfo: topic.getName=${topic.getName} " +
          f"topic.getPartyId=${topic.getPartyId}"
        desc = desc + locationInfo
      }
      desc = desc + f"\nexception trans path: ${host}(${myPartyId})"
    }
    desc
  }

  def throwableToException(t: Throwable, topic: Topic = null): StatusRuntimeException = {
    //if (t.isInstanceOf[StatusRuntimeException]) return t.asInstanceOf[StatusRuntimeException]
    val e = if (t != null) {
      t
    } else {
      new IllegalStateException(s"t is null when throwing exception. myPartyId=${RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()}")
    }
    val desc = genExceptionDescription(e, topic)
    val status = Status.fromThrowable(e).withDescription(desc)
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

  def checkPollingFrameIsException(request: Proxy.PollingFrame): Boolean = {
    PollingMethods.ERROR_POISON.equals(request.getMethod)
  }

  def genExceptionToNextSite(request: Proxy.Packet, t: Throwable = null): Proxy.Packet = {
    // good data
    if (!checkPacketIsException(request) && t == null) return request

    // gen exception info

    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    var descException = s"Error from partyId=${myPartyId}:\n-------------\n"
    if (t != null) {
      descException = genExceptionDescription(t, request.getHeader.getDst)
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
      nextData = s"${nextData} --> ${host} \n == exception divider == \n ${descException}"
      //nextData = nextData + f"--> $host" + "\n ==exception divider== \n" + descException
    } else {
      nextData = descException
    }

    // gen a new packet to next site
    Proxy.Packet.newBuilder()
      .setHeader(request.getHeader)
      .setBody(Proxy.Data.newBuilder()
        .setKey("[roll site transfer exception]")
        .setValue(ByteString.copyFromUtf8(nextData)))
      .build()
  }

  // TODO:0: add site info
  def genExceptionPollingFrame(t: Throwable): PollingFrame = {
    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    var desc = s"Error from partyId=${myPartyId}:\n-------------\n"

    Proxy.PollingFrame.newBuilder()
      .setMethod(PollingMethods.ERROR_POISON)
      .setDesc(desc + ErrorUtils.getStackTraceString(t))
      .build()
  }
}
