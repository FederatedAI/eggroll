package com.webank.eggroll.rollsite

import com.webank.ai.eggroll.api.networking.proxy.Proxy.Topic
import com.webank.eggroll.core.util.RuntimeUtils
import io.grpc.{Status, StatusRuntimeException}
import org.apache.commons.lang3.exception.ExceptionUtils


object ExceptionTransferHelp {
  def throwableToException(throwable: Throwable, topic: Topic = null): StatusRuntimeException = {
    val locMsg = throwable.getLocalizedMessage
    val stackInfo = ExceptionUtils.getStackTrace(throwable)
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
    val status = Status.fromThrowable(throwable).withDescription(desc)
    status.asRuntimeException()
  }
}
