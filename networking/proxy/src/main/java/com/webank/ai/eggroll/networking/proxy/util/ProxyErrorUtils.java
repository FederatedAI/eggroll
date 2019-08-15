/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
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

package com.webank.ai.eggroll.networking.proxy.util;

import com.webank.ai.eggroll.networking.proxy.model.ProxyServerConf;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ProxyErrorUtils {
    @Autowired
    private ProxyServerConf proxyServerConf;

    public StatusRuntimeException toGrpcRuntimeException(Throwable throwable) {
        StatusRuntimeException result = null;

        if (throwable instanceof StatusRuntimeException) {
            result = (StatusRuntimeException) throwable;
        } else {
            result = Status.INTERNAL
                    .withCause(throwable)
                    .withDescription("" + proxyServerConf.getIp() + ":" + proxyServerConf.getPort() + ": " + ExceptionUtils.getStackTrace(throwable))
                    .asRuntimeException();
        }

        return result;
    }
}
