package com.webank.eggroll.clustermanager.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import org.apache.ibatis.annotations.CacheNamespace;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface SessionOptionMapper extends BaseMapper<SessionOption> {
}
