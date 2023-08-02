package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ResourceStateMechine extends AbstractStateMachine<ErProcessor>{
    @Override
    public String getLockKey(ErProcessor processor) {
        return Long.toString(processor.getServerNodeId());
    }

    @Override
    public void doChangeStatus(ErProcessor erProcessor, String preStateParam, String desStateParam) {

    }

    @Override
    public ErProcessor prepare(ErProcessor erProcessor) {
        return erProcessor;
    }
}