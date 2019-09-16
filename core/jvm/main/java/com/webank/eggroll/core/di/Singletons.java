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

package com.webank.eggroll.core.di;

import com.google.common.base.Preconditions;
import com.webank.eggroll.core.error.handler.DefaultLoggingErrorHandler;
import com.webank.eggroll.core.error.handler.ErrorHandler;
import com.webank.eggroll.core.util.ReflectionUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Singletons {

  private static final Object singletonPoolLock = new Object();
  private static final ConcurrentHashMap<String, Object> singletonPool = new ConcurrentHashMap<>();
  private static final ErrorHandler errorHandler = new DefaultLoggingErrorHandler();
  private static final Logger LOGGER = LogManager.getLogger();

  @SuppressWarnings("unchecked")
  public static <T> T get(Class<T> clazz, Object... initArgs)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Preconditions.checkNotNull(clazz);
    if (!singletonPool.containsKey(clazz.getCanonicalName())) {
      synchronized (singletonPoolLock) {
        if (!singletonPool.containsKey(clazz.getCanonicalName())) {
          T singleton = ReflectionUtils.newInstance(clazz, initArgs);
          singletonPool.put(clazz.getCanonicalName(), singleton);
        }
      }
    }

    return (T) singletonPool.get(clazz.getCanonicalName());
  }

  public static <T> T getNoCheck(Class<T> clazz, Object... initArgs) {
    T result = null;

    try {
      result = get(clazz, initArgs);
    } catch (ReflectiveOperationException e) {
      errorHandler.handleError("Error in getting singleton for " + clazz.getCanonicalName(), e);
    }

    return result;
  }
}
