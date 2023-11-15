package org.fedai.eggroll.webapp.dao.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.EggRollBaseServiceImpl;
import org.fedai.eggroll.clustermanager.entity.SessionOption;
import org.fedai.eggroll.webapp.dao.mapper.SessionOptionMapper;

import java.util.List;


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
