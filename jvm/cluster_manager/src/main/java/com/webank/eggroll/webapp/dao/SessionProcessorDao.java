package com.webank.eggroll.webapp.dao;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import java.util.List;

public class SessionProcessorDao {


    @Inject
    SessionProcessorService sessionProcessorService;

    public List<SessionProcessor> getData(int page, int pageSize) {
//        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        IPage<SessionProcessor> pageStats = new Page<SessionProcessor>();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        List<SessionProcessor> data = this.sessionProcessorService.page(pageStats).getRecords();
        return data;
    }
}
