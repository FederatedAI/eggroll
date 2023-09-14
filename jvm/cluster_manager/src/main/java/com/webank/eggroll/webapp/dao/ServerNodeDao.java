package com.webank.eggroll.webapp.dao;

import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.ServerNode;

import java.util.List;

public class ServerNodeDao {

    @Inject
    ServerNodeService serverNodeService;

    public List<ServerNode> getData(int page, int pageSize) {
        PageHelper.startPage(page, pageSize);
        return this.serverNodeService.list();
    }

}
