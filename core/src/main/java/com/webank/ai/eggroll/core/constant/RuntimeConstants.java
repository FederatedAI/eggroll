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

package com.webank.ai.eggroll.core.constant;

import com.webank.ai.eggroll.api.core.BasicMeta;

import java.util.concurrent.TimeUnit;

public class RuntimeConstants {
    public static final String HOST_LANGUAGE = "JVM";
    public static final String LOCALHOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int STORAGE_SERVICE_PORT = 7778;
    public static final int DEFAULT_WAIT_TIME = 10;
    public static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.HOURS;
    private static final String DEFAULT_DATA_DIR = "/tmp/testEggRoll";

    public static BasicMeta.Endpoint getLocalEndpoint(int port) {
        BasicMeta.Endpoint.Builder builder
                = BasicMeta.Endpoint.newBuilder()
                //.setHostname("localhost")
                .setIp("127.0.0.1")
                .setPort(port);

        return builder.build();
    }

    public static String getDefaultDataDir() {
        return DEFAULT_DATA_DIR;
    }
}
