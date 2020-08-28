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

import java.util.concurrent.TimeUnit

import io.grpc.stub.{ClientCallStreamObserver, ServerCallStreamObserver, StreamObserver}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util.{Logging, ToStringUtils}


class ForwardUnaryCallToPollingReqSO(prevRespSO: StreamObserver[Proxy.Packet])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private var nextReqSO: UnaryCallPollingReqSO = _
  private var nextRespSO: ServerCallStreamObserver[Proxy.PollingFrame] = _

  private val self = this
  private val pollingFrameBuilder = Proxy.PollingFrame.newBuilder()
  private var pollingFrameSeq = 0

  private def ensureInit(req: Proxy.Packet): Unit = {
    if (inited) return

    metadata = req.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    val dstPartyId = rsHeader.dstPartyId

    nextReqSO = PollingHelper.getUnaryCallPollingReqSO(dstPartyId, 1, TimeUnit.HOURS)
    nextRespSO = nextReqSO.pollingRespSO
    pollingFrameBuilder.setMethod("push")

    inited = true
  }

  override def onNext(req: Proxy.Packet): Unit = {
    ensureInit(req)

    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setPacket(req)

    val nextFrame = pollingFrameBuilder.build()
    nextRespSO.onNext(nextFrame)
  }

  override def onError(t: Throwable): Unit = {
    logError(s"[FORWARD][SERVER][UNARYCALL2POLLING] onError. rsKey=${rsKey}", t)
    prevRespSO.onError(TransferExceptionUtils.throwableToException(t))
    if (nextRespSO != null && nextRespSO.isReady) {
      nextRespSO.onError(TransferExceptionUtils.throwableToException(t))
    }
  }

  override def onCompleted(): Unit = {
    nextRespSO.onCompleted()
    logDebug(s"[FORWARD][UNARYCALL2POLLING] onCompleted. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}
