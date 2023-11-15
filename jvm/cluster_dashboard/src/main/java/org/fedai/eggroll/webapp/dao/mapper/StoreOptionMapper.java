package org.fedai.eggroll.webapp.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.CacheNamespace;
import org.fedai.eggroll.clustermanager.entity.StoreOption;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface StoreOptionMapper extends BaseMapper<StoreOption> {
}
