package org.fedai.eggroll.core.pojo;


public interface RpcMessage {

    byte[] serialize();

    void deserialize(byte[] data);


}