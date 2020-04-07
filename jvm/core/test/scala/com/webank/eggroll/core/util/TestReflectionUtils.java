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

package com.webank.eggroll.core.util;

import com.webank.eggroll.core.grpc.observer.BaseCallerResponseStreamObserver;
import java.lang.reflect.Constructor;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

public class TestReflectionUtils {

  @Test
  public void testFindDeclaredConstructorWithOneParameter() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1);

    Constructor<BaseCallerResponseStreamObserver> constructor = ReflectionUtils
        .findDeclaredConstructor(BaseCallerResponseStreamObserver.class, countDownLatch);
    assert (constructor != null);
  }
}
