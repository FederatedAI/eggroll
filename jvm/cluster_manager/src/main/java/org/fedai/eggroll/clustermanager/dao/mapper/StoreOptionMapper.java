package org.fedai.eggroll.clustermanager.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.fedai.eggroll.clustermanager.entity.StoreOption;
import org.apache.ibatis.annotations.CacheNamespace;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface StoreOptionMapper extends BaseMapper<StoreOption> {
}
