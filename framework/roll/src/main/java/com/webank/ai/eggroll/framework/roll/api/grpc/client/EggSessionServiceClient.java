package com.webank.ai.eggroll.framework.roll.api.grpc.client;


import com.webank.ai.eggroll.api.computing.ComputingBasic;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.framework.egg.NodeManager;
import com.webank.ai.eggroll.api.framework.egg.SessionServiceGrpc;
import com.webank.ai.eggroll.core.api.grpc.client.GrpcAsyncClientContext;
import com.webank.ai.eggroll.core.api.grpc.client.GrpcStreamingClientTemplate;
import com.webank.ai.eggroll.core.api.grpc.observer.CallerWithSameTypeDelayedResultResponseStreamObserver;
import com.webank.ai.eggroll.core.constant.RuntimeConstants;
import com.webank.ai.eggroll.core.factory.GrpcCallModelFactory;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.model.DelayedResult;
import com.webank.ai.eggroll.core.model.impl.SingleDelayedResult;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import com.webank.ai.eggroll.framework.roll.api.grpc.observer.processor.node.EggNodeServiceEndpointToEndpointResponseObserver;
import com.webank.ai.eggroll.framework.roll.api.grpc.observer.processor.node.EggSessionServiceSessionInfoToSessionInfoResponseObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;

@Component
@Scope("prototype")
public class EggSessionServiceClient {
    private static final Logger LOGGER = LogManager.getLogger();

    @Autowired
    private TypeConversionUtils typeConversionUtils;
    @Autowired
    private GrpcCallModelFactory<SessionServiceGrpc.SessionServiceStub, BasicMeta.SessionInfo, BasicMeta.SessionInfo> ssTemplateFactory;
    @Autowired
    private GrpcCallModelFactory<SessionServiceGrpc.SessionServiceStub, NodeManager.ComputingEngineRequest, ComputingBasic.ComputingEngineDescriptor> ccTemplateFactory;
    @Autowired
    private GrpcCallModelFactory templateFactory;

    public BasicMeta.SessionInfo getOrCreateSession(BasicMeta.SessionInfo request, Node node) {
        return getOrCreateSession(request, typeConversionUtils.toEndpoint(node));
    }

    public BasicMeta.SessionInfo getOrCreateSession(BasicMeta.SessionInfo request, BasicMeta.Endpoint endpoint) {
        GrpcAsyncClientContext<SessionServiceGrpc.SessionServiceStub, BasicMeta.SessionInfo, BasicMeta.SessionInfo> context
                = ssTemplateFactory.createEndpointToEndpointContext(SessionServiceGrpc.SessionServiceStub.class);

        DelayedResult<BasicMeta.SessionInfo> delayedResult = new SingleDelayedResult<>();

        context.setLatchInitCount(1)
                .setEndpoint(endpoint)
                .setFinishTimeout(RuntimeConstants.DEFAULT_WAIT_TIME, RuntimeConstants.DEFAULT_TIMEUNIT)
                .setCalleeStreamingMethodInvoker(SessionServiceGrpc.SessionServiceStub::getOrCreateSession)
                .setCallerStreamObserverClassAndArguments(EggSessionServiceSessionInfoToSessionInfoResponseObserver.class, delayedResult);

        GrpcStreamingClientTemplate<SessionServiceGrpc.SessionServiceStub, BasicMeta.SessionInfo, BasicMeta.SessionInfo> template
                = ssTemplateFactory.createEndpointToEndpointTemplate();

        template.setGrpcAsyncClientContext(context);

        BasicMeta.SessionInfo result;

        try {
            result = template.calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public BasicMeta.SessionInfo stopSession(BasicMeta.SessionInfo request, Node node) {
        return stopSession(request, typeConversionUtils.toEndpoint(node));
    }

    public BasicMeta.SessionInfo stopSession(BasicMeta.SessionInfo request, BasicMeta.Endpoint endpoint) {
        GrpcAsyncClientContext<SessionServiceGrpc.SessionServiceStub, BasicMeta.SessionInfo, BasicMeta.SessionInfo> context
                = ssTemplateFactory.createEndpointToEndpointContext(SessionServiceGrpc.SessionServiceStub.class);

        DelayedResult<BasicMeta.SessionInfo> delayedResult = new SingleDelayedResult<>();

        context.setLatchInitCount(1)
                .setEndpoint(endpoint)
                .setFinishTimeout(RuntimeConstants.DEFAULT_WAIT_TIME, RuntimeConstants.DEFAULT_TIMEUNIT)
                .setCalleeStreamingMethodInvoker(SessionServiceGrpc.SessionServiceStub::stopSession)
                .setCallerStreamObserverClassAndArguments(EggSessionServiceSessionInfoToSessionInfoResponseObserver.class, delayedResult);

        GrpcStreamingClientTemplate<SessionServiceGrpc.SessionServiceStub, BasicMeta.SessionInfo, BasicMeta.SessionInfo> template
                = ssTemplateFactory.createEndpointToEndpointTemplate();

        template.setGrpcAsyncClientContext(context);

        BasicMeta.SessionInfo result;

        try {
            result = template.calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public ComputingEngine getComputingEngine(BasicMeta.SessionInfo sessionInfo, Node node) {
        return getComputingEngine(sessionInfo, typeConversionUtils.toEndpoint(node));
    }

    public ComputingEngine getComputingEngine(BasicMeta.SessionInfo sessionInfo, BasicMeta.Endpoint endpoint) {
        NodeManager.ComputingEngineRequest request = NodeManager.ComputingEngineRequest.newBuilder()
                .setSession(sessionInfo)
                .setComputingEngineDesc(ComputingBasic.ComputingEngineDescriptor.newBuilder()
                        .setEndpoint(endpoint).setType(ComputingBasic.ComputingEngineType.EGGROLL).build())
                .build();

        GrpcAsyncClientContext<SessionServiceGrpc.SessionServiceStub, NodeManager.ComputingEngineRequest, ComputingBasic.ComputingEngineDescriptor> context
                = templateFactory.createEndpointToEndpointContext(SessionServiceGrpc.SessionServiceStub.class);

        DelayedResult<ComputingBasic.ComputingEngineDescriptor> delayedResult = new SingleDelayedResult<>();

        context.setLatchInitCount(1)
                .setEndpoint(endpoint)
                .setFinishTimeout(RuntimeConstants.DEFAULT_WAIT_TIME, RuntimeConstants.DEFAULT_TIMEUNIT)
                .setCalleeStreamingMethodInvoker(SessionServiceGrpc.SessionServiceStub::getComputingEngine)
                .setCallerStreamObserverClassAndArguments(EggSessionServiceComputingEngineRequestToComputingEngineDescriptorStreamObserver.class, delayedResult);

        GrpcStreamingClientTemplate<SessionServiceGrpc.SessionServiceStub, NodeManager.ComputingEngineRequest, ComputingBasic.ComputingEngineDescriptor> template
                = templateFactory.createEndpointToEndpointTemplate();

        template.setGrpcAsyncClientContext(context);

        ComputingEngine result;

        try {
            result = ComputingEngine.fromProtobuf(template.calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult));
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @Component
    @Scope("prototype")
    public static class EggSessionServiceComputingEngineRequestToComputingEngineDescriptorStreamObserver
            extends CallerWithSameTypeDelayedResultResponseStreamObserver<NodeManager.ComputingEngineRequest, ComputingBasic.ComputingEngineDescriptor> {

        public EggSessionServiceComputingEngineRequestToComputingEngineDescriptorStreamObserver(
                CountDownLatch finishLatch, DelayedResult<ComputingBasic.ComputingEngineDescriptor> delayedResult) {
            super(finishLatch, delayedResult);
        }
    }
}
