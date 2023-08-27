package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.inject.Singleton;
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
@Singleton
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
            erProcessors.add(rs.toErProcessor());
        }
        return erProcessors;
    }
}
