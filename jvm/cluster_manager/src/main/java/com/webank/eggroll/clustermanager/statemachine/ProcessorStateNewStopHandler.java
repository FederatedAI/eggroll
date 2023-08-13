package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import org.springframework.stereotype.Service;

@Service
public class ProcessorStateNewStopHandler   extends  AbstractProcessorStateHandler {

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return null;
    }
}
