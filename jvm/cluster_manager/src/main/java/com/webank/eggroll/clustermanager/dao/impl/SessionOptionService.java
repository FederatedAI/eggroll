package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.utils.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.SessionOptionMapper;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.apache.commons.lang3.StringUtils;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


@Singleton
public class SessionOptionService extends EggRollBaseServiceImpl<SessionOptionMapper, SessionOption> {


    @Inject
    SessionProcessorService sessionProcessorService;

    public List<SessionOption> getSessionOptions(String sessionId) {
        return this.baseMapper.selectList(new LambdaQueryWrapper<SessionOption>()
                .eq(SessionOption::getSessionId, sessionId));
    }


    public void removeBySessionId(String sessionId) {
        QueryWrapper<SessionOption> removeWrapper = new QueryWrapper<>();
        removeWrapper.lambda().eq(SessionOption::getSessionId, sessionId);
        this.remove(removeWrapper);
    }
}
