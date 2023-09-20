package com.webank.eggroll.webapp.dao;


import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;

import java.util.List;

public class SessionMainDao {
    @Inject
    SessionMainService sessionMainService;
    public List<SessionMain> getData(int page, int pageSize) {
        PageHelper.startPage(page, pageSize);
        return this.sessionMainService.list();
    }
}
