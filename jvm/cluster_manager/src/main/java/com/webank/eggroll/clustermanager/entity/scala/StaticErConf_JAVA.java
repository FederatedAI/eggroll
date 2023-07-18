package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.clustermanager.config.ErConf_JAVA;

public class StaticErConf_JAVA extends ErConf_JAVA {
    private static int port = -1;
    private static String moduleName;

    public static void setPort(int port) {
        if (StaticErConf_JAVA.port == -1) {
            StaticErConf_JAVA.port = port;
        } else {
            throw new IllegalStateException("port has already been set");
        }
    }

    public static int getPort() {
        return port;
    }

    public static void setModuleName(String moduleName) {
        if (StaticErConf_JAVA.moduleName == null) {
            moduleName = moduleName;
        } else {
            throw new IllegalStateException("module name has already been set");
        }
    }

    public static String getModuleName() {
        return StaticErConf_JAVA.moduleName;
    }
}