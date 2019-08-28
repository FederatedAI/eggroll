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

package com.webank.ai.eggroll.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectionUtils {

  public static <T> Constructor<T> findDeclaredConstructor(Class<T> clazz, Object... cArgs)
      throws NoSuchMethodException {
    Class<?>[] cArgsType = new Class<?>[cArgs.length];

    int i = 0;
    for (Object cArg : cArgs) {
      if (cArg != null) {
        cArgsType[i] = cArg.getClass();
      } else {
        cArgsType[i] = Object.class;
      }
      ++i;
    }

    Constructor<T> result = clazz.getDeclaredConstructor(cArgsType);

    return result;
  }

  public static <T> T newInstance(Constructor<T> constructor, Object... initArgs)
      throws IllegalAccessException, InvocationTargetException, InstantiationException {
    return constructor.newInstance(initArgs);
  }

  public static <T> T newInstance(Class<T> clazz, Object... initArgs)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<T> constructor = findDeclaredConstructor(clazz, initArgs);

    return constructor.newInstance(initArgs);
  }
}
