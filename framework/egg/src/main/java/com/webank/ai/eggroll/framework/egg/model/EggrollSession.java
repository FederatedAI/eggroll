package com.webank.ai.eggroll.framework.egg.model;

import com.google.common.collect.Lists;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class EggrollSession implements Cloneable, Closeable {
    private final String sessionId;
    private final BasicMeta.SessionInfo sessionInfo;
    private @GuardedBy("computingEnginesLock") List<ComputingEngine> computingEngines;
    private AtomicInteger computingEngineIndex;
    private final Object computingEnginesLock;
    private AtomicReference<Status> status;

    public EggrollSession(BasicMeta.SessionInfo sessionInfo) {
        this.sessionId = sessionInfo.getSessionId();
        this.sessionInfo = sessionInfo;
        this.computingEngines = new ArrayList<>();
        this.computingEnginesLock = new Object();
        this.computingEngineIndex = new AtomicInteger(0);
        this.status = new AtomicReference<>(Status.NOT_INITED);
    }

    public BasicMeta.SessionInfo getSessionInfo() {
        return sessionInfo.toBuilder().build();
    }

    public ComputingEngine getComputingEngine() {
        synchronized (computingEnginesLock) {
            int engineCount = computingEngines.size();
            if (engineCount <= 0) {
                return null;
            }
            int index = computingEngineIndex.getAndIncrement() % engineCount;
            return computingEngines.get(index);
        }
    }

    public List<ComputingEngine> getAllComputingEngines() {
        synchronized (computingEnginesLock) {
            List<ComputingEngine> duplicate = Lists.newArrayList(computingEngines);
            return duplicate;
        }
    }

    public EggrollSession addComputingEngine(ComputingEngine computingEngine) {
        synchronized (computingEnginesLock) {
            computingEngines.add(computingEngine);
        }
        return this;
    }

    public ComputingEngine removeComputingEngine(ComputingEngine computingEngine) {
        synchronized (computingEnginesLock) {
            ComputingEngine result = null;
            if (computingEngine == null) {
                return result;
            }

            boolean removeResult = computingEngines.remove(computingEngine);
            if (removeResult) {
                result = computingEngine;
            }
            return result;
        }
    }

    public void updateStatus(Status status) {
        Status curStatus = this.status.get();
        if (curStatus == Status.CLOSED) {
            throw new IllegalStateException("Cannot update a closed session");
        }
        this.status.set(status);
    }

    @Override
    public void close() {
        status.compareAndSet(Status.CLOSING, Status.CLOSED);
    }

    public static enum Status {
        NOT_INITED("not_inited"),
        INITING("initing"),
        RUNNING("running"),
        CLOSING("closing"),
        CLOSED("closed");

        private String name;

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
