package com.eggroll.core.pojo;


public interface RpcMessage extends BaseSerializable, BaseDeserializable {
    default String rpcMessageType() {
        throw new UnsupportedOperationException("Method rpcMessageType() not implemented");
    }
}