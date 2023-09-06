package com.webank.eggroll.webapplication.dao;


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
        IPage pageStats = new Page();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        List data = this.sessionProcessorService.list(pageStats);
        return data;
    }
}
