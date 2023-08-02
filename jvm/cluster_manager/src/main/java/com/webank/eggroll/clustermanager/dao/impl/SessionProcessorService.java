package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SessionProcessorService extends EggRollBaseServiceImpl<SessionProcessorMapper, SessionProcessor> {

    public List<ErProcessor> doQueryProcessor(ErProcessor erProcessor) {
        return doQueryProcessor(erProcessor, null);
    }

    public List<ErProcessor> doQueryProcessor(ErProcessor erProcessor, Map<String, String> extention) {
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
        List<ErProcessor> erProcessors = new ArrayList<>();
        for (SessionProcessor rs : processorList) {
            ErEndpoint commandEndpoint = null;
            if (StringUtils.isNotBlank(rs.getCommandEndpoint())) {
                commandEndpoint = new ErEndpoint(rs.getCommandEndpoint());
            }

            ErEndpoint transferEndpoint = null;
            if (StringUtils.isNotBlank(rs.getTransferEndpoint())) {
                transferEndpoint = new ErEndpoint(rs.getTransferEndpoint());
            }


            ErProcessor erProcessorNode = new ErProcessor();
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
