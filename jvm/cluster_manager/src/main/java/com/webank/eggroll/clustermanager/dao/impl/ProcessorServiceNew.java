package com.webank.eggroll.clustermanager.dao.impl;

import com.google.gson.Gson;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.core.meta.ErProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class ProcessorServiceNew {

    Gson  gson = new Gson();

    @Autowired
    SessionProcessorMapper  sessionProcessorMapper;

    public void  createProcessor(ErProcessor  erProcessor){
//        (Long processorId, String sessionId, Integer serverNodeId, String processorType, String status, String tag, String commandEndpoint, String transferEndpoint, String processorOption, Integer pid, Date
//        createdAt, Date updatedAt) {


//        val result = dbc.update(conn, sql, proc.sessionId, proc.serverNodeId, proc.processorType, ProcessorStatus.NEW, proc.tag,if (proc.commandEndpoint != null) proc.commandEndpoint.toString else "",
//        if (proc.transferEndpoint != null) proc.transferEndpoint.toString else "",if(proc.options!=null) gson.toJson(proc.options) else "")
        SessionProcessor  sessionProcessor =  new SessionProcessor();
        sessionProcessor.setSessionId(erProcessor.sessionId());
        sessionProcessor.setServerNodeId(Long.valueOf(erProcessor.serverNodeId()).intValue());
        sessionProcessor.setProcessorType(erProcessor.processorType());
        sessionProcessor.setStatus(erProcessor.status());
        sessionProcessor.setTag(erProcessor.tag());
        if(erProcessor.commandEndpoint()!=null)
            sessionProcessor.setCommandEndpoint(erProcessor.commandEndpoint().toString());
        if(erProcessor.transferEndpoint()!=null)
            sessionProcessor.setTransferEndpoint(erProcessor.transferEndpoint().toString());
        if(erProcessor.options()!=null)
            sessionProcessor.setProcessorOption(gson.toJson(erProcessor.options()));


        sessionProcessorMapper.insert(sessionProcessor);

    }



}
