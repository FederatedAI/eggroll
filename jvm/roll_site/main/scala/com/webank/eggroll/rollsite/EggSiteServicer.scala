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

import java.util
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.Proxy.PollingFrame
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.constant.{CoreConfKeys, RollSiteConfKeys}
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util._
import com.webank.eggroll.rollsite.utils.ToAuditString
import io.grpc.{Context, Deadline}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager


class EggSiteServicer extends DataTransferServiceGrpc.DataTransferServiceImplBase with Logging {
  private val AUDIT = LogManager.getLogger("audit")
  /**
   */
  override def push(responseObserver: StreamObserver[Proxy.Metadata]): StreamObserver[Proxy.Packet] = {
    logDebug("[PUSH][SERVER] request received")
    new DispatchPushReqSO(responseObserver)
  }

  /**
   */
  override def polling(pollingRespSO: StreamObserver[Proxy.PollingFrame]): StreamObserver[Proxy.PollingFrame] = {
    logDebug("[POLLING][SERVER] request received")
    new DispatchPollingReqSO(pollingRespSO.asInstanceOf[ServerCallStreamObserver[Proxy.PollingFrame]])
    //new MockPollingReqSO(pollingRespSO.asInstanceOf[ServerCallStreamObserver[Proxy.PollingFrame]])
  }

  /**
   */
  override def unaryCall(req: Proxy.Packet,
                         respSO: StreamObserver[Proxy.Packet]): Unit = {
    /**
     * Check if dst is myself.
     *   - yes -> check command to see what the request wants.
     *   - no -> forwards it to the next hop synchronously.
     */
    logTrace(f"[UNARYCALL][SERVER] EggSiteServicer.unaryCall calling")
    val metadata = req.getHeader
    val oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)
    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    val rsKey = rollSiteHeader.getRsKey()
    val context = Context.current()
    val startDeadline = context.getDeadline
    var endDeadline: Deadline = null
    var isPolling = false

    try {
      val dstPartyId = metadata.getDst.getPartyId
      val dstRole = metadata.getDst.getRole
      val logMsg = s"[UNARYCALL][SERVER] unaryCall request received. rsKey=${rsKey}, metadata=${oneLineStringMetadata}, deadline=${startDeadline}"

      val endpoint = Router.query(dstPartyId, dstRole).point
      val dstIsPolling = Router.query(dstPartyId, dstRole).isPolling

      val auditTopics = RollSiteConfKeys.EGGROLL_ROLLSITE_AUDIT_TOPICS.get()
      if (auditTopics != null
        && (util.Arrays.asList(auditTopics).contains(req.getHeader.getSrc.getRole)
        || util.Arrays.asList(auditTopics).contains(req.getHeader.getDst.getRole))) {
        AUDIT.info(ToAuditString.toOneLineString(req.getHeader, "|"))
      }

      logTrace(f"[UNARYCALL][SERVER] EggSiteServicer dst host=${endpoint.host}, dst port=${endpoint.port}")
      val result: Proxy.Packet = if (endpoint.host == RollSiteConfKeys.EGGROLL_ROLLSITE_HOST.get()
        && (endpoint.port == RollSiteConfKeys.EGGROLL_ROLLSITE_PORT.get().toInt
        || endpoint.port == RollSiteConfKeys.EGGROLL_ROLLSITE_SECURE_PORT.get().toInt)) {
        logDebug(s"${logMsg}, hop=SINK")
        processCommand(req, respSO)
      } else {
        if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_SERVER_ENABLED.get().toBoolean && dstIsPolling) {
          logDebug(s"${logMsg}, hop=POLLING_SERVER")
          val reqPollingFrame = Proxy.PollingFrame.newBuilder()
            .setMethod(PollingMethods.UNARY_CALL)
            .setPacket(req)
            .setSeq(1)
            .build()

          var pollingExchanger: PollingExchanger = null
          var i = 0
          val partyId = dstPartyId
          while (pollingExchanger == null) {

            pollingExchanger = PollingExchanger.getPollingExchangerQueue(partyId).poll(
              RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_Q_POLL_INTERVAL_SEC.get().toLong, TimeUnit.SECONDS)
            logTrace(s"unary call getting pollingExchanger from queue. partyId=${partyId}, i=${i}, isNull=${pollingExchanger == null}")
            i += 1
          }
          pollingExchanger.setMethod(PollingMethods.UNARY_CALL)

          PollingExchanger.offer(reqPollingFrame, pollingExchanger.respQ, "EggSiteServicer.unaryCall offering to respQ, ")

          val pollingResult: PollingFrame = PollingExchanger.poll(pollingExchanger.reqQ,
            "EggSiteServicer.unaryCall polling from reqQ, ")

          logTrace(f"[UNARYCALL][SERVER] EggSiteServicer do polling finished.")
          isPolling = true
          pollingResult.getPacket
        } else {
          logDebug(s"${logMsg}, hop=FORWARD")
          var isSecure = Router.query(dstPartyId).isSecure
          val caCrt = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH.get()

          // use secure channel conditions:
          // 1 include crt file.
          // 2 packet have diff src and dst party.
          // 3 point is secure
          isSecure = if (!StringUtils.isBlank(caCrt)
            && req.getHeader.getDst.getPartyId != req.getHeader.getSrc.getPartyId) isSecure else false
          val channel = GrpcClientUtils.getChannel(endpoint, isSecure)
          val stub = DataTransferServiceGrpc.newBlockingStub(channel)
          stub.unaryCall(req)
        }
      }
      respSO.onNext(result)
      respSO.onCompleted()
      endDeadline = context.getDeadline

      if (!isPolling) {
        logDebug(s"[UNARYCALL][SERVER] EggSiteServicer.unaryCall called. rsKey=${rsKey}, " +
          s"metadata=${oneLineStringMetadata}, " +
          s"isCancelled=${context.isCancelled}, " +
          s"deadline=${if (endDeadline != null) endDeadline else null}, " +
          s"isExpired=${if (endDeadline != null) endDeadline.isExpired else false}")
      }
    } catch {
      case t: Throwable =>
        val wrapped = TransferExceptionUtils.throwableToException(t)
        logError(s"[UNARYCALL][SERVER] onError. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", wrapped)
        respSO.onError(wrapped)
    }
  }

  private def processCommand(request: Proxy.Packet, responseSO: StreamObserver[Proxy.Packet]): Proxy.Packet = {
    logDebug(s"packet to myself. request: ${ToStringUtils.toOneLineString(request)}")

    val operator = request.getHeader.getOperator

    val result: Proxy.Packet = operator match {
      case "get_route_table" =>
        getRouteTable(request)
      case "set_route_table" =>
        setRouteTable(request)
      case "ping" =>
        ping(request)
      case _ =>
        val e = new NotImplementedError(s"operation ${operator} not supported")

        // TODO:0: optimise log
        throw e
    }

    result
  }

  private def verifyToken(request: Proxy.Packet): Boolean = {
    val data = request.getBody.getValue.toStringUtf8 // salt + json data
    val routerKey = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_KEY.get()
    val md5Token = request.getBody.getKey
    val checkMd5 = Util.hashMD5(data + routerKey)
    logDebug(f"routerKey=${routerKey}, md5Token=${md5Token}, checkMd5=${checkMd5}")
    md5Token == checkMd5
  }

  private def checkWhiteList(): Boolean = {
    val srcIp = AddrAuthServerInterceptor.REMOTE_ADDR.get.asInstanceOf[String]
    logDebug(s"scrIp=${srcIp}")
    WhiteList.check(srcIp)
  }

  private def setRouteTable(request: Proxy.Packet): Proxy.Packet = {
    if (!verifyToken(request)) {
      logWarning("setRouteTable failed. Token verification failed.")
      val data = Proxy.Data.newBuilder.setValue(
        ByteString.copyFromUtf8("setRouteTable failed. Token verification failed.")).build
      return Proxy.Packet.newBuilder().setBody(data).build
    }

    if (!checkWhiteList()){
      logWarning("setRouteTable failed, Src ip not included in whitelist.")
      val data = Proxy.Data.newBuilder.setValue(
        ByteString.copyFromUtf8("setRouteTable failed, Src ip not included in whitelist.")).build
      return Proxy.Packet.newBuilder().setBody(data).build
    }

    val jsonString = request.getBody.getValue.substring(13).toStringUtf8
    val routerFilePath = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_PATH.get()
    Router.update(jsonString, routerFilePath)
    val data = Proxy.Data.newBuilder.setValue(ByteString.copyFromUtf8("setRouteTable finished")).build
    Proxy.Packet.newBuilder().setBody(data).build
  }

  private def getRouteTable(request: Proxy.Packet): Proxy.Packet = {
    if (!verifyToken(request)) {
      logWarning("getRouteTable failed. Token verification failed.")
      val data = Proxy.Data.newBuilder.setValue(
        ByteString.copyFromUtf8("getRouteTable failed. Token verification failed.")).build
      return Proxy.Packet.newBuilder().setBody(data).build
    }

    if (!checkWhiteList()){
      logWarning("getRouteTable failed, Src ip not included in whitelist.")
      val data = Proxy.Data.newBuilder.setValue(
        ByteString.copyFromUtf8("getRouteTable failed, Src ip not included in whitelist.")).build
      return Proxy.Packet.newBuilder().setBody(data).build
    }

    val routerFilePath = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_PATH.get()
    val jsonString = Router.get(routerFilePath)
    val data = Proxy.Data.newBuilder.setValue(ByteString.copyFromUtf8(jsonString)).build
    Proxy.Packet.newBuilder().setBody(data).build
  }

  private def ping(request: Proxy.Packet): Proxy.Packet = {
    Thread.sleep(1000)
    val header = Proxy.Metadata.newBuilder().setOperator("pong").build()
    Proxy.Packet.newBuilder().setHeader(header).build()
  }

}