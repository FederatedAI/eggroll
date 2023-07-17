package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.datastructure.RpcMessage;

public interface NetworkingRpcMessage_JAVA extends RpcMessage {
    @Override
    default String rpcMessageType() {
        return "Networking";
    }
}