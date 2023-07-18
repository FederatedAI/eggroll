package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.clustermanager.config.ErConf_JAVA;
import com.webank.eggroll.core.constant.SessionConfKeys;

import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unused")
public class RuntimeErConf_JAVA extends ErConf_JAVA {
    public RuntimeErConf_JAVA(Properties prop) {
        super();
        getConf().putAll(prop);
    }

    public RuntimeErConf_JAVA(ErSessionMeta_JAVA sessionMeta) {
        super();
        for (Map.Entry<String, String> entry : sessionMeta.getOptions().entrySet()) {
            getConf().put(entry.getKey(), entry.getValue());
        }
        getConf().put(SessionConfKeys.CONFKEY_SESSION_ID(), sessionMeta.getId());
        getConf().put(SessionConfKeys.CONFKEY_SESSION_NAME(), sessionMeta.getName());
    }

    public RuntimeErConf_JAVA(StartContainersRequest_JAVA startContainersRequest) {
        super();
        for (Map.Entry<String, String> entry : startContainersRequest.options.entrySet()) {
            getConf().put(entry.getKey(), entry.getValue());
        }
        getConf().put(SessionConfKeys.CONFKEY_SESSION_ID(), startContainersRequest.sessionId);
        getConf().put(SessionConfKeys.CONFKEY_SESSION_NAME(), startContainersRequest.name);
    }

    public RuntimeErConf_JAVA(Map<String, String> conf) {
        super();
        getConf().putAll(conf);
    }

    public static int getPort() {
        return StaticErConf_JAVA.getPort();
    }

    public static String getModuleName() {
        return StaticErConf_JAVA.getModuleName();
    }
}