package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionOptionMapper;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SessionOptionService extends EggRollBaseServiceImpl<SessionOptionMapper, SessionOption>{

    public List<SessionOption> getSessionOptions(String sessionId){
        return this.baseMapper.selectList(new LambdaQueryWrapper<SessionOption>()
                .eq(SessionOption::getSessionId,sessionId));
    }
}
