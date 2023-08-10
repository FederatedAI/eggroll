package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.common.collect.Maps;
import com.webank.eggroll.clustermanager.dao.mapper.SessionMainMapper;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
public class SessionMainService extends EggRollBaseServiceImpl<SessionMainMapper, SessionMain>{

    @Autowired
    SessionOptionService  sessionOptionService;
    @Autowired
    ProcessorService   processorService;
    @Autowired
    SessionProcessorService sessionProcessorService;

    public ErSessionMeta getSession(String sessionId) {
        SessionOption sessionOption = new SessionOption();
        sessionOption.setSessionId(sessionId);
        List<SessionOption> optList = sessionOptionService.list(sessionOption);
        Map<String, String> opts = optList.stream().collect(Collectors.toMap(SessionOption::getName, SessionOption::getData));

        SessionProcessor sessionProcessor = new SessionProcessor();
        sessionProcessor.setSessionId(sessionId);
        List<SessionProcessor> processorList = sessionProcessorService.list(sessionProcessor);
        List<ErProcessor> procs = new ArrayList<>();
        for (SessionProcessor processor : processorList) {
            procs.add(processor.toErProcessor());
        }
        ErSessionMeta session = this.getSessionMain(sessionId);
        session.setOptions(opts);
        session.setProcessors(procs);
        return session;
    }

    public ErSessionMeta getSession(String sessionId,boolean  withProcessor,boolean withOption,boolean  withResource){
        ErSessionMeta  erSessionMeta = null;
        SessionMain  sessionMain = this.baseMapper.selectById(sessionId);
        if(sessionMain!=null) {
            erSessionMeta = sessionMain.toErSessionMeta();
            if(withProcessor) {
                erSessionMeta.setProcessors(processorService.getProcessorBySession(sessionId,withResource));
            }
            if(withOption) {
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

    public ErSessionMeta getSessionMain(String sessionId){
        SessionMain sessionMain = this.getById(sessionId);
        return sessionMain.toErSessionMeta();
    }

    @Transactional
    public void updateSessionMain(ErSessionMeta sessionMeta, Consumer<ErSessionMeta> afterCall) {
        UpdateWrapper<SessionMain> updateWrapper = new UpdateWrapper<>();
        updateWrapper.lambda().set(SessionMain::getName,sessionMeta.getName())
                .set(SessionMain::getStatus,sessionMeta.getStatus())
                .set(SessionMain::getTag,sessionMeta.getTag())
                .set(SessionMain::getActiveProcCount,sessionMeta.getActiveProcCount())
                .eq(SessionMain::getSessionId,sessionMeta.getId());
        this.update(updateWrapper);
        if(afterCall!=null){
            afterCall.accept(sessionMeta);
        }
    }

    public List<ErSessionMeta> getSessionMainsByStatus(List<String> status){
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda().in(SessionMain::getStatus,status);
        List<SessionMain> essionMainList = this.list(queryWrapper);
        List<ErSessionMeta> result = new ArrayList<>();
        for (SessionMain sessionMain : essionMainList) {
            result.add(sessionMain.toErSessionMeta());
        }
        return result;
    }

}
