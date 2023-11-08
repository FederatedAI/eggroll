package com.webank.eggroll.webapp.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.webank.eggroll.clustermanager.entity.StorePartition;
import org.apache.ibatis.annotations.CacheNamespace;

@CacheNamespace(flushInterval = 60000, size = 128, readWrite = false)
public interface StorePartitionMapper extends BaseMapper<StorePartition> {
}
