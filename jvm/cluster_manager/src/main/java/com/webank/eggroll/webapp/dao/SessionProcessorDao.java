package com.webank.eggroll.webapp.dao;


import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import java.util.List;

public class SessionProcessorDao {
    @Inject
    SessionProcessorService sessionProcessorService;

    public List<SessionProcessor> getData(int page, int pageSize) {
        PageHelper.startPage(page, pageSize);
        return this.sessionProcessorService.list();
    }
}
