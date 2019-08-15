package com.webank.ai.eggroll.framework.roll.api.grpc.observer.processor.node;

import com.webank.ai.eggroll.api.computing.ComputingBasic;
import com.webank.ai.eggroll.api.framework.egg.NodeManager;
import com.webank.ai.eggroll.core.api.grpc.observer.CallerWithSameTypeDelayedResultResponseStreamObserver;
import com.webank.ai.eggroll.core.model.DelayedResult;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/*
@Component
@Scope("prototype")
public class EggSessionServiceComputingEngineRequestToComputingEngineDescriptorStreamObserver
        extends CallerWithSameTypeDelayedResultResponseStreamObserver<NodeManager.ComputingEngineRequest, ComputingBasic.ComputingEngine> {

    public EggSessionServiceComputingEngineRequestToComputingEngineDescriptorStreamObserver(
            CountDownLatch finishLatch, DelayedResult<ComputingBasic.ComputingEngine> delayedResult) {
        super(finishLatch, delayedResult);
    }
}*/
