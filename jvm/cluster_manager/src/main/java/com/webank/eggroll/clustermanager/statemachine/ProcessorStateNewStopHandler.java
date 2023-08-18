package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class ProcessorStateNewStopHandler extends AbstractProcessorStateHandler {

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        this.updateState(data, desStateParam);
        return data;
    }
}
