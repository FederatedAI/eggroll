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

package com.webank.ai.eggroll.networking.proxy.grpc.service;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.api.networking.proxy.RouteServiceGrpc;
import com.webank.ai.eggroll.core.utils.ErrorUtils;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.networking.proxy.service.FdnRouter;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class RouteServerImpl extends RouteServiceGrpc.RouteServiceImplBase {
    private static final Logger LOGGER = LogManager.getLogger(RouteServerImpl.class);
    @Autowired
    private FdnRouter fdnRouter;
    @Autowired
    private ToStringUtils toStringUtils;
    @Autowired
    private ErrorUtils errorUtils;

    @Override
    public void query(Proxy.Topic request, StreamObserver<BasicMeta.Endpoint> responseObserver) {
        String requestString = toStringUtils.toOneLineString(request);
        LOGGER.info("[ROUTE] querying route for topic: {}", requestString);
        BasicMeta.Endpoint result = fdnRouter.route(request);

        if (result == null) {
            NullPointerException e = new NullPointerException("no valid route for topic: " + requestString);
            responseObserver.onError(errorUtils.toGrpcRuntimeException(e));
        }

        LOGGER.info("[ROUTE] querying route result for topic: {}, result: {}", requestString, toStringUtils.toOneLineString(result));
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}
