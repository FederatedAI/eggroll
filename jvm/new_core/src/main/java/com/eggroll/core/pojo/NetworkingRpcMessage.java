package com.eggroll.core.pojo;


public interface NetworkingRpcMessage extends RpcMessage {
    default String rpcMessageType() {
        return "Networking";
    }
}