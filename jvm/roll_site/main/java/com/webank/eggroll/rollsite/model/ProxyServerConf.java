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

package com.webank.eggroll.rollsite.model;


import com.webank.eggroll.rollsite.factory.PipeFactory;
import com.webank.eggroll.rollsite.infra.Pipe;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ProxyServerConf {
    private String ip;
    private int port;
    private int securePort;
    private long pushRetryCount;
    private long unaryCallRetryCount;
    private long pullTimeout;
    private String partyId;
    private String role;

    private String gatewayIp;
    private int gatewayPort;
    private String gatewayPartyId;
    private String gatewayRole;

    private String routeTablePath;
    private String[] whiteList;
    private String[] auditTopics;

    private boolean isSecureServer;
    private String serverCrtPath;
    private String serverKeyPath;

    private boolean isSecureClient;
    private String caCrtPath;

    private Pipe pipe;
    private PipeFactory pipeFactory;

    private String coordinator;
    private String logPropertiesPath;

    private boolean isAuditEnabled;
    private boolean isNeighbourInsecureChannelEnabled;

    private boolean isDebugEnabled;
    private boolean isCompatibleEnabled;
    private Properties properties;

    public ProxyServerConf() {
        properties = new Properties();
    }

    @Override
    public String toString() {
        return "ServerConf{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", routeTablePath='" + routeTablePath + '\'' +
                ", isSecureServer=" + isSecureServer +
                ", serverCrtPath='" + serverCrtPath + '\'' +
                ", serverKeyPath='" + serverKeyPath + '\'' +
                ", isSecureClient=" + isSecureClient +
                ", caCrtPath='" + caCrtPath + '\'' +
                ", pipe=" + pipe +
                ", pipeFactory=" + pipeFactory +
                ", coordinator='" + coordinator + '\'' +
                ", logPropertiesPath='" + logPropertiesPath + '\'' +
                ", isAuditEnabled=" + isAuditEnabled +
                ", isNeighbourInsecureChannelEnabled=" + isNeighbourInsecureChannelEnabled +
                ", isDebugEnabled=" + isDebugEnabled +
                '}';
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSecurePort() {
        return securePort;
    }

    public void setSecurePort(int securePort) {
        this.securePort = securePort;
    }

    public void setPartyId(String partyId) {
        this.partyId = partyId;
    }

    public String getPartyId() {
        return partyId;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }

    public String getGatewayIp() {
        return gatewayIp;
    }

    public void setGatewayIp(String ip) {
        this.gatewayIp = ip;
    }

    public int getGatewayPort() {
        return gatewayPort;
    }

    public void setGatewayPort(int port) {
        this.gatewayPort = port;
    }

    public void setGatewayPartyId(String partyId) {
        this.gatewayPartyId = partyId;
    }

    public String getGatewayPartyId() {
        return gatewayPartyId;
    }

    public void setGatewayRole(String role) {
        this.gatewayRole = role;
    }

    public String getGatewayRole() {
        return gatewayRole;
    }

    public String getRouteTablePath() {
        return routeTablePath;
    }

    public void setRouteTablePath(String routeTablePath) {
        this.routeTablePath = routeTablePath;
    }

    public String[] getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(String whiteList) {
        String[] whiteListArray = whiteList.split("\\,");
        this.whiteList = whiteListArray;
    }

    public boolean isSecureServer() {
        return isSecureServer;
    }

    public void setSecureServer(boolean secureServer) {
        isSecureServer = secureServer;
    }

    public String getServerCrtPath() {
        return serverCrtPath;
    }

    public void setServerCrtPath(String serverCrtPath) {
        this.serverCrtPath = serverCrtPath;
    }

    public String getServerKeyPath() {
        return serverKeyPath;
    }

    public void setServerKeyPath(String serverKeyPath) {
        this.serverKeyPath = serverKeyPath;
    }

    public boolean isSecureClient() {
        return isSecureClient;
    }

    public void setSecureClient(boolean secureClient) {
        isSecureClient = secureClient;
    }

    public String getCaCrtPath() {
        return caCrtPath;
    }

    public void setCaCrtPath(String caCrtPath) {
        this.caCrtPath = caCrtPath;
    }

    public Pipe getPipe() {
        return pipe;
    }

    public void setPipe(Pipe pipe) {
        this.pipe = pipe;
    }

    public PipeFactory getPipeFactory() {
        return pipeFactory;
    }

    public void setPipeFactory(PipeFactory pipeFactory) {
        this.pipeFactory = pipeFactory;
    }

    public String getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(String coordinator) {
        this.coordinator = coordinator;
    }

    public String getLogPropertiesPath() {
        return logPropertiesPath;
    }

    public void setLogPropertiesPath(String logPropertiesPath) {
        this.logPropertiesPath = logPropertiesPath;
    }

    public boolean isAuditEnabled() {
        return isAuditEnabled;
    }

    public void setAuditEnabled(boolean auditEnabled) {
        this.isAuditEnabled = auditEnabled;
    }

    public String[] getAuditTopics() {
        return auditTopics;
    }

    public void setAuditTopics(String auditTopics) {
        String[] auditTopicsArray = auditTopics.split("\\,");
        this.auditTopics = auditTopicsArray;
    }

    public boolean isNeighbourInsecureChannelEnabled() {
        return isNeighbourInsecureChannelEnabled;
    }

    public void setNeighbourInsecureChannelEnabled(boolean neighbourInsecureChannelEnabled) {
        isNeighbourInsecureChannelEnabled = neighbourInsecureChannelEnabled;
    }

    public boolean isDebugEnabled() {
        return isDebugEnabled;
    }

    public void setDebugEnabled(boolean debugEnabled) {
        isDebugEnabled = debugEnabled;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public boolean isCompatibleEnabled() {
        return isCompatibleEnabled;
    }

    public ProxyServerConf setCompatibleEnabled(boolean compatibleEnabled) {
        isCompatibleEnabled = compatibleEnabled;
        return this;
    }
}
