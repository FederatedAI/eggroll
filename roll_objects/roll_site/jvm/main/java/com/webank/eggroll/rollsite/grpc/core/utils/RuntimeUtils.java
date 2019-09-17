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

package com.webank.eggroll.rollsite.grpc.core.utils;

import com.webank.eggroll.rollsite.grpc.core.server.DefaultServerConf;
import com.webank.eggroll.rollsite.grpc.core.server.ServerConf;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RuntimeUtils {
    @Autowired
    private ServerConf serverConf;

    private static String myIpAndPort = null;
    private static String siteLocalAddress = null;

    @PostConstruct
    private void init() {
        if (serverConf == null) {
            serverConf = new DefaultServerConf();
        }
    }

    public String getMySiteLocalAddress() {
        if (siteLocalAddress == null) {
            Enumeration<NetworkInterface> networkInterfaces = null;
            try {
                networkInterfaces = NetworkInterface.getNetworkInterfaces();

                for (NetworkInterface ni : Collections.list(networkInterfaces)) {
                    Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
                    for (InetAddress ia : Collections.list(inetAddresses)) {
                        if (ia.isSiteLocalAddress()) {
                            siteLocalAddress = StringUtils.substringAfterLast(ia.toString(), "/");
                        }
                    }
                }
            } catch (SocketException e) {
                siteLocalAddress = "127.0.0.1";
            }
        }

        return siteLocalAddress;
    }

    public String getMySiteLocalIpAndPort() {
        if (myIpAndPort == null) {
            myIpAndPort = getMySiteLocalAddress() + ":" + serverConf.getPort();
        }

        return myIpAndPort;
    }

    public boolean isPortAvailable(int port) {
        if (port <= 0) {
            return false;
        }
        try (ServerSocket ignored = new ServerSocket(port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    public long getPidOfProcess(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }
}
