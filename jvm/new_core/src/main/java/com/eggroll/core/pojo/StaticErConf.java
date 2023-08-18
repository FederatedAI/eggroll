package com.eggroll.core.pojo;


import com.eggroll.core.config.ErConf;

public class StaticErConf extends ErConf {
    private int port = -1;
    private String moduleName;

    public StaticErConf setPort(int port) {
        if (this.port == -1) {
            this.port = port;
        } else {
            throw new IllegalStateException("port has already been set");
        }
        return this;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setModuleName(String moduleName) {
        if (this.moduleName == null) {
            this.moduleName = moduleName;
        } else {
            throw new IllegalStateException("module name has already been set");
        }
    }

    @Override
    public String getModuleName() {
        return moduleName;
    }
}
