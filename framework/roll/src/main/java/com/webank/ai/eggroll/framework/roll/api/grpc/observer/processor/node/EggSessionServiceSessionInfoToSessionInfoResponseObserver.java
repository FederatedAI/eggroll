package com.webank.ai.eggroll.framework.roll.api.grpc.observer.processor.node;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.api.grpc.observer.CallerWithSameTypeDelayedResultResponseStreamObserver;
import com.webank.ai.eggroll.core.model.DelayedResult;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
@Scope("prototype")
public class EggSessionServiceSessionInfoToSessionInfoResponseObserver
        extends CallerWithSameTypeDelayedResultResponseStreamObserver<BasicMeta.SessionInfo, BasicMeta.SessionInfo> {
    public EggSessionServiceSessionInfoToSessionInfoResponseObserver(CountDownLatch finishLatch,
                                                                     DelayedResult<BasicMeta.SessionInfo> delayedResult) {
        super(finishLatch, delayedResult);
    }
}
