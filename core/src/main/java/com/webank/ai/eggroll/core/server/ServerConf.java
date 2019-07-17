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

package com.webank.ai.eggroll.core.server;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;

import java.util.Properties;

public interface ServerConf {
    public Properties getProperties();

    public ServerConf addService(BindableService service);

    public ServerConf addService(ServerServiceDefinition service);

    public String getIp();

    public int getPort();

    public String getProperty(String key);

    public String getProperty(String key, String defaultValue);
}
