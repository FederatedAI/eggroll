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
 */

package com.webank.eggroll.core.command

import com.webank.eggroll.core.command.CommandPbSerdes._
import com.webank.eggroll.core.constant.ModuleConstants
import com.webank.eggroll.core.grpc.server.GrpcServerWrapper
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import io.grpc.stub.StreamObserver

import scala.collection.JavaConverters._


class CommandService extends CommandServiceGrpc.CommandServiceImplBase with Logging {
  private val grpcServerWrapper = new GrpcServerWrapper();

  /**
   * Delegates commands to registered service.
   *
   * @param request
   * @param responseObserver
   */
  override def call(request: Command.CommandRequest,
                    responseObserver: StreamObserver[Command.CommandResponse]): Unit = {
    grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () => {
      logInfo(s"${ModuleConstants.COMMAND_WITH_BRACKETS} received: ${ToStringUtils.toOneLineString(request)}")
      val command: ErCommandRequest = request.fromProto()
      val commandUri = new CommandURI(command)

      val result: Array[Byte] = CommandRouter.dispatch(commandUri.getRoute(), request.getArgsList.toArray(), request.getKwargsMap.asScala)

      val response: ErCommandResponse = ErCommandResponse(seq = request.getSeq,
        data = result)
      responseObserver.onNext(response.toProto())
      responseObserver.onCompleted()
    });
  }
}