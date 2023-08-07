package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;

public class ProcessorStateRunningHandler   extends  AbstractProcessorStateHandler {
    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return null;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return null;
    }
}
