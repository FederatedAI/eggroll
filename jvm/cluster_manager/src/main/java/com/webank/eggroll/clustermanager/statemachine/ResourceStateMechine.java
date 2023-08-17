package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ResourceStateMechine extends AbstractStateMachine<ErProcessor> implements InitializingBean {

    @Autowired
    ResourceStateHandler resourceStateHandler;

    @Override
    String buildStateChangeLine(Context context, ErProcessor erProcessor, String preStateParam, String desStateParam) {
        String  stateLine =  preStateParam+"_"+desStateParam;
        return  stateLine;
    }

    @Override
    //这里需要锁nodeid  ，因为资源按节点分配
    public String getLockKey(ErProcessor processor) {
        return processor.getServerNodeId().toString();
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander("init_pre_allocated",resourceStateHandler);
        this.registeStateHander("pre_allocated_allocated",resourceStateHandler);
        this.registeStateHander("pre_allocated_allocate_failed",resourceStateHandler);
        this.registeStateHander("allocated_return",resourceStateHandler);
    }
}
