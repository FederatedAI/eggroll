/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.rollsite

import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import io.grpc.stub.StreamObserver


class ForwardUnaryCallToPollingReqSO(unaryCallRespSO: StreamObserver[Proxy.Packet])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private val self = this
  private val pollingFrameBuilder = Proxy.PollingFrame.newBuilder()
  private var pollingFrameSeq = 0

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    logTrace(s"onInit calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    inited = true
    logDebug(s"onInit called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.Packet): Unit = {
    logTrace(s"onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInited(req)

    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setPacket(req)
    val nextFrame = pollingFrameBuilder.build()

    PollingHelper.pollingRespQueue.put(nextFrame)
    logTrace(s"onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    unaryCallRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")

    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setMethod("finish_unary_call")
    PollingHelper.pollingRespQueue.put(pollingFrameBuilder.build())

    val pollingReq = PollingHelper.pollingReqQueue.take()

    unaryCallRespSO.onNext(pollingReq.getPacket)
    unaryCallRespSO.onCompleted()

    logTrace(s"onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}
