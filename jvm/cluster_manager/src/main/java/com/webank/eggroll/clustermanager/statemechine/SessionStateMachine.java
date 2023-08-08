package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ProcessorType;
import com.eggroll.core.constant.SessionEvents;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;

import com.google.common.collect.Lists;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
@Service
public class SessionStateMachine extends AbstractStateMachine<ErSessionMeta> implements InitializingBean {
    @Autowired
    SessionMainService   sessionMainService;
    @Autowired
    ProcessorStateMechine  processorStateMechine;
    @Autowired
    SessionKillHandler  sessionKillHandler;
    @Autowired
    SessionStopHandler  sessionStopHandler;
    @Autowired
    SessionActiveHandler  sessionActiveHandler;
    @Autowired
    SessionCreateHandler  sessionCreateHandler;

    @Override
    String buildStateChangeLine(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        String  line= "";
        ErSessionMeta  sessionInDb = sessionMainService.getSession(erSessionMeta.getId(),true,false,false);
        if(sessionInDb!=null){
            context.putData(Dict.SESSION_IN_DB,sessionInDb);
        }
        if(StringUtils.isEmpty(preStateParam))
        {
            if(sessionInDb==null){
                preStateParam ="";
            }else{
                preStateParam =  sessionInDb.getStatus();
            }
        }
        line= preStateParam+"_"+desStateParam;
        return  line;
    }

    @Override
    public String getLockKey(ErSessionMeta erSessionMeta) {
        return erSessionMeta.getId();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander("_NEW",sessionCreateHandler);
        this.registeStateHander("NEW_ACTIVE",sessionActiveHandler);
        this.registeStateHander("NEW_KILLED",sessionKillHandler);
        this.registeStateHander("NEW_ERROR",sessionKillHandler);
        this.registeStateHander("ACTIVE_KILLED",sessionKillHandler);
        this.registeStateHander("ACTIVE_ERROR",sessionKillHandler);
        this.registeStateHander("ACTIVE_CLOSED",sessionStopHandler);
    }
}
