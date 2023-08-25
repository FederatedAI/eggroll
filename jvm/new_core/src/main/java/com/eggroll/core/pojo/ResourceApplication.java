package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.exceptions.ErSessionException;
import javafx.util.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class ResourceApplication {
    private String sessionId;
    private String sessionName;
    private List<ErProcessor> processors;
    private String sortByResourceType;
    private boolean needDispatch;
    private String dispatchStrategy;
    private String resourceExhaustedStrategy;
    private boolean allowExhausted;
    private List<Pair<ErProcessor, ErServerNode>> resourceDispatch;
    private CountDownLatch resourceLatch;
    private Integer timeout;
    private Long submitTimeStamp;
    private AtomicInteger waitingCount;
    private AtomicInteger status;
    private List<String> processorTypes;
    private Map<String, String> options;

    public ResourceApplication() {
        this.sessionName = StringConstants.EMPTY;
        this.processors = new ArrayList<>();
        this.sortByResourceType = Dict.VCPU_CORE;
        this.needDispatch = true;
        this.dispatchStrategy = Dict.SINGLE_NODE_FIRST;
        this.resourceExhaustedStrategy = Dict.WAITING;
        this.allowExhausted = false;
        this.resourceDispatch = new ArrayList<>();
        this.resourceLatch = new CountDownLatch(1);
        this.timeout = 0;
        this.submitTimeStamp = 0L;
        this.waitingCount = new AtomicInteger(1);
        this.status = new AtomicInteger(0);
        this.processorTypes = new ArrayList<>();
        this.options = new HashMap<>();
    }

    public List<Pair<ErProcessor, ErServerNode>> getResult() throws InterruptedException, ErSessionException {
        try {
            if (timeout > 0) {
                boolean alreadyGet = resourceLatch.await(timeout, TimeUnit.MILLISECONDS);
                if (!alreadyGet) {
                    throw new ErSessionException("dispatch resource timeout");
                }
            } else {
                resourceLatch.await();
            }
            return resourceDispatch;
        } finally {
            waitingCount.decrementAndGet();
        }
    }

    public void countDown() {
        resourceLatch.countDown();
    }
}
