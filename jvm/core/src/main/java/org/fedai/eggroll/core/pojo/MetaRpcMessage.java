package org.fedai.eggroll.core.pojo;


public interface MetaRpcMessage extends BaseSerializable, BaseDeserializable {
    default String rpcMessageType() {
        throw new UnsupportedOperationException("Method rpcMessageType() not implemented");
    }
}