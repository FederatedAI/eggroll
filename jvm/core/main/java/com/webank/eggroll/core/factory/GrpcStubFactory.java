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

package com.webank.eggroll.core.factory;

import com.google.common.base.Preconditions;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.transfer.GrpcClientUtils;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.lang3.reflect.MethodUtils;
import scala.collection.immutable.HashMap;

public class GrpcStubFactory {

  private static final String asyncStubMethodName = "newStub";
  private static final String blockingStubMethodName = "newBlockingStub";


  public AbstractStub createGrpcStub(boolean isAsync, Class grpcClass,
      ManagedChannel managedChannel) {
    String methodName = null;
    if (isAsync) {
      methodName = asyncStubMethodName;
    } else {
      methodName = blockingStubMethodName;
    }

    AbstractStub result = null;
    try {
      result = (AbstractStub) MethodUtils
          .invokeStaticMethod(grpcClass, methodName, managedChannel);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("error creating stub", e);
    }

    return result;
  }

  public AbstractStub createGrpcStub(boolean isAsync, Class grpcClass, ErEndpoint endpoint,
      boolean isSecure) {
    Preconditions.checkNotNull(endpoint, "Endpoint cannot be null");
    ManagedChannel managedChannel = GrpcClientUtils.getChannel(endpoint, isSecure, new HashMap<>());

    return createGrpcStub(isAsync, grpcClass, managedChannel);
  }
}
