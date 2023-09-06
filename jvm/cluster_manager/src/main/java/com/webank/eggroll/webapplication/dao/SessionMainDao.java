package com.webank.eggroll.webapplication.dao;


import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;

import java.util.List;

public class SessionMainDao {
    @Inject
    SessionMainService sessionMainService;
    public List<SessionMain> getData(int page, int pageSize) {
        IPage pageStats = new Page();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        List data = this.sessionMainService.list(pageStats);
        return data;
    }
}
