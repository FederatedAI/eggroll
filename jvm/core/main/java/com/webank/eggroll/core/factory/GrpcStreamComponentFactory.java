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

import com.webank.eggroll.core.grpc.processor.StreamProcessor;
import com.webank.eggroll.core.util.ReflectionUtils;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;

public class GrpcStreamComponentFactory {

  public <T extends StreamObserver> T createCallerResponseStreamObserver(Class<T> soClass,
      CountDownLatch finishLatch, Object... specificInitArgs) {
    return createStreamComponentInternal(soClass, specificInitArgs, finishLatch);
  }

  public <T extends StreamProcessor> T createStreamProcessor(Class<T> spClass,
      StreamObserver streamObserver, Object... specificInitArgs) {
    return createStreamComponentInternal(spClass, specificInitArgs, streamObserver);
  }

  private <T> T createStreamComponentInternal(Class<T> soClass, Object[] specificInitArgs,
      Object... fixedInitArgs) {
    T result = null;

    try {
      result = ReflectionUtils
          .newInstance(soClass, concatInitArgs(specificInitArgs, fixedInitArgs));
    } catch (NoSuchMethodException | IllegalAccessException
        | InvocationTargetException | InstantiationException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    return result;
  }

  private Object[] concatInitArgs(Object[] specificInitArgs, Object... fixedInitArgs) {
    if (specificInitArgs == null || specificInitArgs.length <= 0) {
      return fixedInitArgs;
    }

    int specificInitArgsLength = specificInitArgs.length;
    int fixedInitArgsLength = fixedInitArgs.length;

    Object[] result = new Object[specificInitArgsLength + fixedInitArgsLength];

    int i = 0;
    for (; i < fixedInitArgsLength; ++i) {
      result[i] = fixedInitArgs[i];
    }

    for (int j = 0; j < specificInitArgsLength; ++i, ++j) {
      result[i] = specificInitArgs[j];
    }

    return result;
  }
}
