package com.eggroll.core.grpc;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ActionType;
import com.eggroll.core.context.Context;
import com.eggroll.core.flow.FlowLogUtil;
import com.eggroll.core.pojo.ErEndpoint;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandClient {
    public byte[] call(Context oriContext, ErEndpoint erEndpoint, String uri, byte[] request) {
        Context context = new Context();
        context.setUri(uri);
        if (oriContext != null) {
            context.setSeq(oriContext.getSeq());
        }
        context.setActionType(ActionType.CLIENT.name());
        context.setEndpoint(erEndpoint);
        try {
            CommandServiceGrpc.CommandServiceBlockingStub stub = CommandServiceGrpc.newBlockingStub(GrpcConnectionFactory.createManagedChannel(erEndpoint, true));
            Command.CommandRequest.Builder requestBuilder = Command.CommandRequest.newBuilder();
            requestBuilder.setId(System.currentTimeMillis() + "").setUri(uri).addArgs(ByteString.copyFrom(request));
            Command.CommandResponse commandResponse = stub.call(requestBuilder.build());
            return commandResponse.getResults(0).toByteArray();
        } catch (Throwable e) {
            log.error("send {} to {} error", uri, erEndpoint, e);
            context.setThrowable(e);
            throw e;
        } finally {
            if (MetaInfo.EGGROLL_FLOWLOG_PRINT_CLIENT) {
                FlowLogUtil.printFlowLog(context);
            }

        }
    }


}
