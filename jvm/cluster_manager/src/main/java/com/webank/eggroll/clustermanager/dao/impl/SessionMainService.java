package com.webank.eggroll.clustermanager.dao.impl;

import com.eggroll.core.pojo.ErSessionMeta;
import com.google.common.collect.Maps;
import com.webank.eggroll.clustermanager.dao.mapper.SessionMainMapper;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SessionMainService extends EggRollBaseServiceImpl<SessionMainMapper, SessionMain>{

    @Autowired
    SessionOptionService  sessionOptionService;
    @Autowired
    ProcessorService   processorService;

    public ErSessionMeta getSession(String sessionId,boolean  recursion){
        ErSessionMeta  erSessionMeta = null;
        SessionMain  sessionMain = this.baseMapper.selectById(sessionId);
        if(sessionMain!=null) {
            erSessionMeta = sessionMain.toErSessionMeta();
            if(recursion) {
                erSessionMeta.setProcessors(processorService.getProcessorBySession(sessionId));
                List<SessionOption> result = sessionOptionService.getSessionOptions(sessionId);
                Map<String, String> optionMap = Maps.newHashMap();
                result.forEach(sessionOption -> {
                    optionMap.put(sessionOption.getName(), sessionOption.getData());
                });
                erSessionMeta.setOptions(optionMap);
            }
        }
        return  erSessionMeta;
    }
}
