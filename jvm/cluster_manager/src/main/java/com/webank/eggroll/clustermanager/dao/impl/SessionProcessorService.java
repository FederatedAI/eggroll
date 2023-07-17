package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.clustermanager.entity.scala.ErEndpoint_JAVA;
import com.webank.eggroll.clustermanager.entity.scala.ErProcessor_JAVA;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SessionProcessorService extends EggRollBaseServiceImpl<SessionProcessorMapper, SessionProcessor> {

    public List<ErProcessor_JAVA> doQueryProcessor(ErProcessor_JAVA erProcessor) {
        return doQueryProcessor(erProcessor, null);
    }

    public List<ErProcessor_JAVA> doQueryProcessor(ErProcessor_JAVA erProcessor, Map<String, String> extention) {
        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        if (erProcessor != null) {
            queryWrapper.lambda().eq(StringUtils.isNotEmpty(erProcessor.getStatus()), SessionProcessor::getStatus, erProcessor.getStatus())
                    .eq(StringUtils.isNotEmpty(erProcessor.getSessionId()), SessionProcessor::getSessionId, erProcessor.getSessionId())
                    .eq(erProcessor.getId() != -1, SessionProcessor::getProcessorId, erProcessor.getId());
        }
        if (extention != null) {
            extention.forEach(queryWrapper::apply);
        }
        List<SessionProcessor> processorList = this.list(queryWrapper);
        List<ErProcessor_JAVA> erProcessors = new ArrayList<>();
        for (SessionProcessor rs : processorList) {
            ErEndpoint_JAVA commandEndpoint = null;
            if (StringUtils.isNotBlank(rs.getCommandEndpoint())) {
                commandEndpoint = ErEndpoint_JAVA.apply(rs.getCommandEndpoint());
            }

            ErEndpoint_JAVA transferEndpoint = null;
            if (StringUtils.isNotBlank(rs.getTransferEndpoint())) {
                transferEndpoint = ErEndpoint_JAVA.apply(rs.getTransferEndpoint());
            }


            ErProcessor_JAVA erProcessorNode = new ErProcessor_JAVA();
            erProcessorNode.setId(rs.getProcessorId());
            erProcessorNode.setSessionId(rs.getSessionId());
            erProcessorNode.setServerNodeId(rs.getServerNodeId());
            erProcessorNode.setProcessorType(rs.getProcessorType());
            erProcessorNode.setStatus(rs.getStatus());
            erProcessorNode.setCommandEndpoint(commandEndpoint);
            erProcessorNode.setTransferEndpoint(transferEndpoint);
            erProcessorNode.setPid(rs.getPid());
            erProcessorNode.setCreatedAt(rs.getCreatedAt());
            erProcessorNode.setUpdatedAt(rs.getUpdatedAt());
            erProcessors.add(erProcessorNode);
        }
        return erProcessors;
    }
}
