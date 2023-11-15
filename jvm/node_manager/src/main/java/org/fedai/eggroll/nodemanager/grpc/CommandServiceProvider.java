package org.fedai.eggroll.nodemanager.grpc;

import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.containers.meta.KillContainersResponse;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.containers.meta.StopContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.AbstractCommandServiceProvider;
import org.fedai.eggroll.core.grpc.Dispatcher;
import org.fedai.eggroll.core.grpc.URI;
import org.fedai.eggroll.core.invoke.InvokeInfo;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.core.utils.NetUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.nodemanager.processor.DefaultProcessorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        return defaultProcessorManager.stopContainers(context, sessionMeta);
    }

    @URI(value = killContainers)
    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        return defaultProcessorManager.killContainers(context, sessionMeta);
    }

    @URI(value = eggpairHeartbeat)
    public ErProcessor heartbeat(Context context, ErProcessor processor) {
        String nodeHost = MetaInfo.CONFKEY_NODE_MANAGER_HOST == null ? NetUtils.getLocalHost(MetaInfo.CONFKEY_NODE_MANAGER_NET_DEVICE) : MetaInfo.CONFKEY_NODE_MANAGER_HOST;
        processor.getCommandEndpoint().setHost(nodeHost);
        processor.getTransferEndpoint().setHost(nodeHost);
        return defaultProcessorManager.heartbeat(context, processor);
    }

    @URI(value = checkNodeProcess)
    public ErProcessor checkNodeProcess(Context context, ErProcessor processor) {
        return defaultProcessorManager.checkNodeProcess(context, processor);
    }


    @URI(value = startFlowJobContainers)
    public StartContainersResponse startFlowJobContainers(Context context, StartFlowContainersRequest startFlowContainersRequest) {
        return defaultProcessorManager.startFlowJobContainers(startFlowContainersRequest);
    }
    
    @URI(value = startJobContainers)
    public StartContainersResponse startJobContainers(Context context, StartContainersRequest startContainersRequest) {
        return defaultProcessorManager.startJobContainers(startContainersRequest);
    }

    @URI(value = stopJobContainers)
    public StopContainersResponse stopJobContainers(Context context, StopContainersRequest stopContainersRequest) {
        return defaultProcessorManager.stopJobContainers(stopContainersRequest);
    }

    @URI(value = killJobContainers)
    public KillContainersResponse killJobContainers(Context context, KillContainersRequest killContainersRequest) {
        return defaultProcessorManager.killJobContainers(killContainersRequest);
    }

    @URI(value = downloadContainers)
    public DownloadContainersResponse downloadContainers(Context context, DownloadContainersRequest downloadContainersRequest) {
        return defaultProcessorManager.downloadContainers(downloadContainersRequest);
    }


    @URI(value = nodeHeartbeat)
    public ErProcessor nodeHeartbeat(Context context, ErProcessor processor) {
        return null;
    }


    @URI(value = nodeMetaInfo)
    public MetaInfoResponse queryNodeMetaInfo(Context context, MetaInfoRequest metaInfoRequest) {
        logger.info("=============queryNodeMetaInfo==============");
        MetaInfoResponse metaInfoResponse = new MetaInfoResponse();
        Map<String,String> metaMap = new HashMap<>();
        Map map = MetaInfo.toMap();
        map.keySet().stream().forEach(key -> {
            String strValue = null;
            Object object = map.get(key);
            if (null != object) {
                strValue = String.valueOf(map.get(key));
            }
            metaMap.put(String.valueOf(key),strValue);
        });
        metaInfoResponse.setMetaMap(metaMap);
        return metaInfoResponse;
    }

}
