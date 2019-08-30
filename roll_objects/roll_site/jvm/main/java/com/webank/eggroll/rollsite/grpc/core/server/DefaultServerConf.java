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

package com.webank.eggroll.rollsite.grpc.core.server;

import com.google.common.collect.Lists;
import com.webank.eggroll.rollsite.grpc.core.utils.PropertyGetter;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import java.util.List;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("serverConf")
public class DefaultServerConf implements ServerConf {
    @Autowired
    private PropertyGetter propertyGetter;

    private String ip;
    private int port;

    private boolean isSecureServer;
    private String serverCrtPath;
    private String serverKeyPath;

    private boolean isSecureClient;
    private String caCrtPath;

    private String partyId;
    private String logPropertiesPath;

    private boolean isDebugEnabled;

    private Properties properties;

    private List<ServerServiceDefinition> serverServiceDefinitions;

    public DefaultServerConf() {
        serverServiceDefinitions = Lists.newLinkedList();
    }

    @Override
    public String getIp() {
        return ip;
    }

    public DefaultServerConf setIp(String ip) {
        this.ip = ip;
        return this;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public DefaultServerConf setPort(int port) {
        this.port = port;
        return this;
    }

    public List<ServerServiceDefinition> getServerServiceDefinitions() {
        return serverServiceDefinitions;
    }

    public DefaultServerConf setServerServiceDefinitions(List<ServerServiceDefinition> serverServiceDefinitions) {
        this.serverServiceDefinitions = serverServiceDefinitions;
        return this;
    }

    @Override
    public DefaultServerConf addService(BindableService service) {
        this.serverServiceDefinitions.add(service.bindService());
        return this;
    }

    @Override
    public DefaultServerConf addService(ServerServiceDefinition service) {
        this.serverServiceDefinitions.add(service);
        return this;
    }

    public boolean isSecureServer() {
        return isSecureServer;
    }

    public DefaultServerConf setSecureServer(boolean secureServer) {
        isSecureServer = secureServer;
        return this;
    }

    public String getServerCrtPath() {
        return serverCrtPath;
    }

    public DefaultServerConf setServerCrtPath(String serverCrtPath) {
        this.serverCrtPath = serverCrtPath;
        return this;
    }

    public String getServerKeyPath() {
        return serverKeyPath;
    }

    public DefaultServerConf setServerKeyPath(String serverKeyPath) {
        this.serverKeyPath = serverKeyPath;
        return this;
    }

    public boolean isSecureClient() {
        return isSecureClient;
    }

    public DefaultServerConf setSecureClient(boolean secureClient) {
        isSecureClient = secureClient;
        return this;
    }

    public String getCaCrtPath() {
        return caCrtPath;
    }

    public DefaultServerConf setCaCrtPath(String caCrtPath) {
        this.caCrtPath = caCrtPath;
        return this;
    }

    public String getPartyId() {
        return partyId;
    }

    public DefaultServerConf setPartyId(String partyId) {
        this.partyId = partyId;
        return this;
    }

    public String getLogPropertiesPath() {
        return logPropertiesPath;
    }

    public DefaultServerConf setLogPropertiesPath(String logPropertiesPath) {
        this.logPropertiesPath = logPropertiesPath;
        return this;
    }

    public boolean isDebugEnabled() {
        return isDebugEnabled;
    }

    public DefaultServerConf setDebugEnabled(boolean debugEnabled) {
        isDebugEnabled = debugEnabled;
        return this;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public DefaultServerConf setProperties(Properties properties) {
        this.properties = properties;
        this.propertyGetter.addSource(properties);
        return this;
    }
}
