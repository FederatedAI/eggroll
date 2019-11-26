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

package com.webank.eggroll.core.grpc.server;

import com.webank.eggroll.core.error.handler.DefaultLoggingErrorHandler;
import com.webank.eggroll.core.error.handler.ErrorHandler;
import com.webank.eggroll.core.util.ErrorUtils;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcServerWrapper {
  private static final Logger LOGGER = LogManager.getLogger();
  private final ErrorHandler errorHandler;

  public GrpcServerWrapper() {
    this(new DefaultLoggingErrorHandler());
  }

  public GrpcServerWrapper(ErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
  }

  public void wrapGrpcServerRunnable(StreamObserver responseObserver, GrpcServerRunnable target) {
    try {
      target.run();
    } catch (Throwable t) {
      errorHandler.handleError(t);

      responseObserver.onError(ErrorUtils.toGrpcRuntimeException(t));
    }
  }

  public <T> T wrapGrpcServerCallable(StreamObserver responseObserver,
      GrpcServerCallable<T> target) {
    T result = null;

    try {
      result = target.call();
    } catch (Throwable t) {
      errorHandler.handleError(t);
      responseObserver.onError(ErrorUtils.toGrpcRuntimeException(t));
    }

    return result;
  }
}
