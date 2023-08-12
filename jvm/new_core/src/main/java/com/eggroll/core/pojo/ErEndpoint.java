package com.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Data
public class ErEndpoint implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErEndpoint.class);
    private String host;
    private int port;

    public ErEndpoint() {

    }

    public ErEndpoint(String url) {
        String[] toks = url.split(":");
        this.host = toks[0];
        this.port = Integer.parseInt(toks[1]);
    }

    public ErEndpoint(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    //        public boolean isValid() {
//            return !StringUtils.isBlank(host) && port > 0;
//        }
//
    public Meta.Endpoint toProto() {
        Meta.Endpoint.Builder builder = Meta.Endpoint.newBuilder()
                .setHost(host)
                .setPort(port);
        return builder.build();
    }

    public static ErEndpoint fromProto(Meta.Endpoint endpoint){
        ErEndpoint erEndpoint = new ErEndpoint();
        erEndpoint.deserialize(endpoint.toByteArray());
        return erEndpoint;
    }


    public static ErEndpoint apply(String url) {
        ErEndpoint erEndpoint = new ErEndpoint();
        if (StringUtils.isNotBlank(url)) {
            String[] split = url.split(":");
            if (split.length >= 2) {
                erEndpoint.setHost(split[0]);
                erEndpoint.setPort(Integer.parseInt(split[1]));
            }
        }
        return erEndpoint;
    }



    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.Endpoint endpoint = Meta.Endpoint.parseFrom(data);
            this.host = endpoint.getHost();
            this.port = endpoint.getPort();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }

    }
}