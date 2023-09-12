package com.webank.eggroll.nodemanager.grpc;

import com.eggroll.core.containers.meta.KillContainersResponse;
import com.eggroll.core.containers.meta.StartContainersResponse;
import com.eggroll.core.containers.meta.StopContainersResponse;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.AbstractCommandServiceProvider;
import com.eggroll.core.grpc.Dispatcher;
import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.eggroll.core.grpc.URI;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import com.webank.eggroll.nodemanager.processor.DefaultProcessorManager;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.grpc.CommandUri.*;

@Singleton
public class CommandServiceProvider extends AbstractCommandServiceProvider {

    Logger logger = LoggerFactory.getLogger(CommandServiceProvider.class);

    @Inject
    DefaultProcessorManager defaultProcessorManager;

    private ConcurrentHashMap<String, InvokeInfo> uriMap = new ConcurrentHashMap();


    @Inject
    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.dispatcher.register(this);
    }


    @URI(value = startContainers)
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        return defaultProcessorManager.startContainers(context, sessionMeta);
    }

    @URI(value = stopContainers)
    public ErSessionMeta stopContainers(Context context,ErSessionMeta sessionMeta) {
        return defaultProcessorManager.stopContainers(context, sessionMeta);
    }

    @URI(value = killContainers)
    public ErSessionMeta killContainers(Context context,ErSessionMeta sessionMeta) {
        return defaultProcessorManager.killContainers(context, sessionMeta);
    }

    @URI(value = eggpairHeartbeat)
    public ErProcessor heartbeat(Context context,ErProcessor processor) {
        return defaultProcessorManager.heartbeat(context, processor);
    }

    @URI(value = checkNodeProcess)
    public ErProcessor checkNodeProcess(Context context,ErProcessor processor) {
        return defaultProcessorManager.checkNodeProcess(context, processor);
    }

    @URI(value = startJobContainers)
    public StartContainersResponse startJobContainers(Context context,StartContainersRequest startContainersRequest) {
        return defaultProcessorManager.startJobContainers(startContainersRequest);
    }

    @URI(value = stopJobContainers)
    public StopContainersResponse stopJobContainers(Context context,StopContainersRequest stopContainersRequest) {
        return defaultProcessorManager.stopJobContainers(stopContainersRequest);
    }

    @URI(value = killJobContainers)
    public KillContainersResponse killJobContainers(Context context,KillContainersRequest killContainersRequest) {
        return defaultProcessorManager.killJobContainers(killContainersRequest);
    }

    @URI(value = downloadContainers)
    public DownloadContainersResponse downloadContainers(Context context,DownloadContainersRequest downloadContainersRequest) {
        return defaultProcessorManager.downloadContainers(downloadContainersRequest);
    }


    @URI(value = nodeHeartbeat)
    public ErProcessor nodeHeartbeat(Context context ,ErProcessor processor) {
        return null;
    }





}
