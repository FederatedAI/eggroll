package com.eggroll.core.containers.container;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.Map;
import java.util.concurrent.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ContainersManager {

    private Map<Long, ContainerLifecycleHandler> handlers;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private final List<Container.ContainerStatusCallback> callbacks = new ArrayList<>();

    public ContainersManager(ExecutorService ec, List<Container.ContainerStatusCallback> callbacks) {
        handlers = new ConcurrentHashMap<>();
        executor = ec;
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleAtFixedRate(
                () -> {
                    handlers.forEach((containerId, handler) -> {
                        if (handler.isPoisoned()) {
                            handler.killContainer();
                        }
                    });
                    handlers.entrySet().removeIf(entry -> entry.getValue().isCompleted());
                }, 0, 1, TimeUnit.MINUTES);
        this.callbacks.addAll(callbacks);
    }

    public static ContainersManagerBuilder builder() {
        return new ContainersManagerBuilder();
    }

    public void addContainer(Long containerId, ContainerTrait container) {

        ContainerLifecycleHandler lifecycleHandler = ContainerLifecycleHandler
                .builder()
                .withContainer(container)
                .withCallbacks(callbacks)
                .build(executor);
        handlers.putIfAbsent(containerId, lifecycleHandler);
    }

    public void startContainer(Long containerId) {
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
//            executor.execute(handler::startContainer);
            handler.startContainer();
        }
    }

    public void stopContainer(Long containerId) {
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
            executor.execute(handler::stopContainer);
        }
    }

    public void killContainer(Long containerId) {
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
            executor.execute(handler::killContainer);
        }
    }

    public void stop() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }
}
