package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.Mapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ProcessorType;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.*;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.NodeResourceMapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionMainMapper;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.clustermanager.entity.SessionRanks;
import com.webank.eggroll.clustermanager.statemachine.SessionStateMachine;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@Singleton
public class SessionMainService extends EggRollBaseServiceImpl<SessionMainMapper, SessionMain> {
    Logger logger = LoggerFactory.getLogger(SessionMainService.class);

    @Inject
    SessionOptionService sessionOptionService;

    @Inject
    SessionProcessorService sessionProcessorService;

    @Inject
    SessionRanksService sessionRanksService;

    @Inject
    SessionStateMachine sessionStateMachine;

    public boolean updateSessionMainActiveCount(String sessionId) {
        List<ErProcessor> processors = sessionProcessorService.getProcessorBySession(sessionId, false);
        logger.info("=============  {}", processors);
        long activeCount = 0;
        for (int i = 0; i < processors.size(); i++) {
            ErProcessor p = processors.get(i);
            if (p.getStatus().equals(ProcessorStatus.RUNNING.name())) {
                activeCount = activeCount + 1;
            }
        }
        logger.info("total {} active {}", processors.size(), activeCount);
        UpdateWrapper<SessionMain> updateWrapper = new UpdateWrapper<>();
        updateWrapper.lambda()
                .set(SessionMain::getActiveProcCount, activeCount)
                .eq(SessionMain::getSessionId, sessionId);
        this.update(updateWrapper);
        return processors.size() != 0 && processors.size() == activeCount;
    }


    public ErSessionMeta getSession(String sessionId) {
        SessionOption sessionOption = new SessionOption();
        sessionOption.setSessionId(sessionId);
        List<SessionOption> optList = sessionOptionService.list(sessionOption);
        Map<String, String> opts = optList.stream().collect(Collectors.toMap(SessionOption::getName, SessionOption::getData));

        List<SessionProcessor> processorList = sessionProcessorService.list(new LambdaQueryWrapper<SessionProcessor>().eq(SessionProcessor::getSessionId,sessionId));
        List<ErProcessor> procs = new ArrayList<>();
        for (SessionProcessor processor : processorList) {
            procs.add(processor.toErProcessor());
        }
        ErSessionMeta session = this.getSessionMain(sessionId);
        if (session != null) {
            session.setOptions(opts);
            session.setProcessors(procs);
        }
        return session;
    }

    public ErSessionMeta getSession(String sessionId, boolean withProcessor, boolean withOption, boolean withResource) {
        ErSessionMeta erSessionMeta = null;
        SessionMain sessionMain = this.getById(sessionId);
        if (sessionMain != null) {
            erSessionMeta = sessionMain.toErSessionMeta();
            if (withProcessor) {
                erSessionMeta.setProcessors(sessionProcessorService.getProcessorBySession(sessionId, withResource));
            }
            if (withOption) {
                List<SessionOption> result = sessionOptionService.getSessionOptions(sessionId);
                Map<String, String> optionMap = Maps.newHashMap();
                result.forEach(sessionOption -> {
                    optionMap.put(sessionOption.getName(), sessionOption.getData());
                });
                erSessionMeta.setOptions(optionMap);
            }
        }
        return erSessionMeta;
    }

    public ErSessionMeta getSessionMain(String sessionId) {
        SessionMain sessionMain = this.getById(sessionId);
        if (sessionMain != null)
            return sessionMain.toErSessionMeta();
        else
            return null;
    }

    @Transactional
    public void updateSessionMain(ErSessionMeta sessionMeta, Consumer<ErSessionMeta> afterCall) {
        UpdateWrapper<SessionMain> updateWrapper = new UpdateWrapper<>();
        updateWrapper.lambda().set(SessionMain::getName, sessionMeta.getName())
                .set(SessionMain::getStatus, sessionMeta.getStatus())
                .set(SessionMain::getTag, sessionMeta.getTag())
                .set(SessionMain::getActiveProcCount, sessionMeta.getActiveProcCount())
                .eq(SessionMain::getSessionId, sessionMeta.getId());
        this.update(updateWrapper);
        if (afterCall != null) {
            afterCall.accept(sessionMeta);
        }
    }

    public List<ErSessionMeta> getSessionMainsByStatus(List<String> status) {
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda().in(SessionMain::getStatus, status);
        List<SessionMain> essionMainList = this.list(queryWrapper);
        List<ErSessionMeta> result = new ArrayList<>();
        for (SessionMain sessionMain : essionMainList) {
            result.add(sessionMain.toErSessionMeta());
        }
        return result;
    }


    @Transactional
    public void register(ErSessionMeta sessionMeta, Boolean replace) {
        String sessionId = sessionMeta.getId();
        Map<String, String> options = sessionMeta.getOptions();
        List<ErProcessor> processors = sessionMeta.getProcessors();

        if (replace) {
            this.removeById(sessionId);
            sessionOptionService.removeBySessionId(sessionId);
            sessionProcessorService.removeBySessionId(sessionId);
        }

        SessionMain sessionMain = new SessionMain(sessionId, sessionMeta.getName(), sessionMeta.getStatus(),
                sessionMeta.getTag(), sessionMeta.getTotalProcCount());

        this.save(sessionMain);

        if (MapUtils.isNotEmpty(options)) {
            List<SessionOption> optionList = new ArrayList<>();
            options.entrySet().forEach(entry -> {
                optionList.add(new SessionOption(sessionId, entry.getKey(), entry.getValue()));
            });
            sessionOptionService.saveBatch(optionList);
        }

        if (CollectionUtils.isNotEmpty(processors)) {
            List<SessionProcessor> processorList = new ArrayList<>();
            processors.forEach(erProcessor -> {
                SessionProcessor sessionProcessor = new SessionProcessor();
                sessionProcessor.setSessionId(sessionId);
                sessionProcessor.setProcessorType(erProcessor.getProcessorType());
                sessionProcessor.setStatus(erProcessor.getStatus());
                sessionProcessor.setTag(erProcessor.getTag());
                if (erProcessor.getServerNodeId() != null) {
                    sessionProcessor.setServerNodeId(erProcessor.getServerNodeId().intValue());
                }
                if (erProcessor.getCommandEndpoint() != null) {
                    sessionProcessor.setCommandEndpoint(erProcessor.getCommandEndpoint().toString());
                }
                if (erProcessor.getTransferEndpoint() != null) {
                    sessionProcessor.setTransferEndpoint(erProcessor.getTransferEndpoint().toString());
                }
                processorList.add(sessionProcessor);

            });
            sessionProcessorService.saveBatch(processorList);
        }
    }

    @Transactional
    public void registerRanks(List<MutableTriple<Long, ErServerNode, DeepspeedContainerConfig>> configs, String sesssionId) {
        for (MutableTriple<Long, ErServerNode, DeepspeedContainerConfig> config : configs) {
            SessionRanks sessionRanks = new SessionRanks();
            sessionRanks.setSessionId(sesssionId);
            sessionRanks.setContainerId(config.getLeft());
            sessionRanks.setServerNodeId(config.getMiddle().getId());
            sessionRanks.setGlobalRank(config.getRight().getRank());
            sessionRanks.setLocalRank(config.getRight().getLocalRank());
            sessionRanksService.save(sessionRanks);
        }
    }

    @Transactional
    public void registerWithResource(ErSessionMeta erSessionMeta) {
//        this.removeById(erSessionMeta.getId());
//        sessionOptionService.remove(new QueryWrapper<SessionOption>().lambda().eq(SessionOption::getSessionId,erSessionMeta.getId()));
//        sessionProcessorService.remove(new QueryWrapper<SessionProcessor>().lambda().eq(SessionProcessor::getSessionId,erSessionMeta.getId()));

//        SessionMain sessionMain = new SessionMain();
//        sessionMain.setSessionId(erSessionMeta.getId());
//        sessionMain.setName(erSessionMeta.getName());
//        sessionMain.setStatus(erSessionMeta.getStatus());
//        sessionMain.setTag(erSessionMeta.getTag());
//        sessionMain.setTotalProcCount(erSessionMeta.getTotalProcCount());
//        sessionMain.setActiveProcCount(0);
//        this.save(sessionMain);
//
//        Map<String, String> opts = erSessionMeta.getOptions();
//        if(opts!=null){
//            opts.forEach((k,v)->{
//                SessionOption sessionOption = new SessionOption();
//                sessionOption.setSessionId(erSessionMeta.getId());
//                sessionOption.setName(k);
//                sessionOption.setData(v);
//                sessionOptionService.save(sessionOption);
//            });
//        }
        Context context = new Context();
        context.putData(Dict.KEY_PROCESSOR_TYPE, ProcessorType.DeepSpeed.name());
        sessionStateMachine.changeStatus(context, erSessionMeta, null, SessionStatus.NEW.name());
    }

}
