package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eggroll.core.pojo.ErProcessor;
import com.google.gson.Gson;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ProcessorService extends EggRollBaseServiceImpl<SessionProcessorMapper,SessionProcessor>{

    Gson  gson = new Gson();

    @Autowired
    SessionProcessorMapper  sessionProcessorMapper;

    public List<ErProcessor> getProcessorBySession(String sessionId){
         return sessionProcessorMapper.selectList(new LambdaQueryWrapper<SessionProcessor>().eq(SessionProcessor::getSessionId,sessionId))
                 .stream().map((x)->{ return x.toErProcessor();}).collect(Collectors.toList());

    }









}
