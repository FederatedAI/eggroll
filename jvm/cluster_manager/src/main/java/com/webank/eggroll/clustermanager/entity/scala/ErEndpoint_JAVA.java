package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class ErEndpoint_JAVA implements NetworkingRpcMessage_JAVA {
    private String host;
    private int port;

    public ErEndpoint_JAVA(String url) {
        String[] toks = url.split(":");
        this.host = toks[0];
        this.port = Integer.parseInt(toks[1]);
    }

    public ErEndpoint_JAVA(String host , Integer port){
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    public boolean isValid() {
        return !StringUtils.isBlank(host) && port > 0;
    }

    public Meta.Endpoint toProto() {
        Meta.Endpoint.Builder builder = Meta.Endpoint.newBuilder()
                .setHost(host)
                .setPort(port);

        return builder.build();
    }
}