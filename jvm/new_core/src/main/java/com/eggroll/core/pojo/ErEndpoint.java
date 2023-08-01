package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;



public  class ErEndpoint {
    private String host;
    private int port;


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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
}