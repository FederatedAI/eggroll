package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.meta.ErProcessor;
import com.webank.eggroll.core.meta.ErResource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SessionProcessorService extends EggRollBaseServiceImpl<SessionProcessorMapper, SessionProcessor> {

    public ErProcessor[] doQueryProcessor(ErProcessor erProcessor) {
        return doQueryProcessor(erProcessor, null);
    }

    public ErProcessor[] doQueryProcessor(ErProcessor erProcessor, Map<String, String> extention) {
        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        if (erProcessor != null) {
            queryWrapper.lambda().eq(StringUtils.isNotEmpty(erProcessor.status()), SessionProcessor::getStatus, erProcessor.status())
                    .eq(StringUtils.isNotEmpty(erProcessor.sessionId()), SessionProcessor::getSessionId, erProcessor.sessionId())
                    .eq(erProcessor.id() != -1, SessionProcessor::getProcessorId, erProcessor.id());
        }
        if (extention != null) {
            extention.forEach(queryWrapper::apply);
        }
        List<SessionProcessor> processorList = this.list(queryWrapper);
        ErProcessor[] erProcessors = new ErProcessor[processorList.size()];
        for (int i = 0; i < processorList.size(); i++) {
            SessionProcessor rs = processorList.get(i);
            ErEndpoint commandEndpoint = null;
            if(StringUtils.isNotBlank(rs.getCommandEndpoint())){
                commandEndpoint = ErEndpoint.apply(rs.getCommandEndpoint());
            }

            ErEndpoint transferEndpoint = null;
            if(StringUtils.isNotBlank(rs.getTransferEndpoint())){
                transferEndpoint = ErEndpoint.apply(rs.getTransferEndpoint());
            }
            ErProcessor erProcessorNode = new ErProcessor(rs.getProcessorId(), rs.getSessionId(),rs.getServerNodeId(), StringConstants.EMPTY()
                    ,rs.getProcessorType(),rs.getStatus(),commandEndpoint,transferEndpoint
                    ,rs.getPid(),new ConcurrentHashMap<>(),StringConstants.EMPTY()
                    ,new ErResource[0],new Timestamp(rs.getCreatedAt().getTime()),new Timestamp(rs.getUpdatedAt().getTime()));
            erProcessors[i] = erProcessorNode;
        }
        return erProcessors;
    }
}
