package com.webank.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;
import com.google.inject.Singleton;

@Singleton
public class ProcessorStateIgnoreHandler extends AbstractProcessorStateHandler {

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

}

