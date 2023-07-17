package com.webank.eggroll.clustermanager.entity.scala;


public interface RpcMessage_JAVA extends BaseSerializable_JAVA, BaseDeserializable_JAVA {
    default String rpcMessageType() {
        throw new UnsupportedOperationException("Method rpcMessageType() not implemented");
    }
}