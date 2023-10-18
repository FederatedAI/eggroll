package com.eggroll.core.containers.container;

import lombok.Data;

import java.util.Optional;


@Data
public class StoreConfig {
    private String host;
    private Integer port;
    private String prefix;

    public StoreConfig(String storeHost, Integer storePort, String storePrefix) {
        this.host = storeHost;
        this.port = storePort;
        this.prefix = storePrefix;
    }
}
