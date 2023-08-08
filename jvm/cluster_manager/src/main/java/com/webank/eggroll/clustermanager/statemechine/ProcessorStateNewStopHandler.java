package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.stereotype.Service;

@Service
public class ProcessorStateNewStopHandler   extends  AbstractProcessorStateHandler {

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return null;
    }
}
