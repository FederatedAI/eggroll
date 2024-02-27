package org.fedai.eggroll.clustermanager.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.CacheNamespace;
import org.fedai.eggroll.clustermanager.entity.SessionOption;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface SessionOptionMapper extends BaseMapper<SessionOption> {
}