package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Singleton;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;

@Singleton
public class ProcessorStateIgnoreHandler extends AbstractProcessorStateHandler {

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

}

