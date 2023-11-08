package com.webank.eggroll.webapp.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.webank.eggroll.clustermanager.entity.StoreOption;
import org.apache.ibatis.annotations.CacheNamespace;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface StoreOptionMapper extends BaseMapper<StoreOption> {
}
