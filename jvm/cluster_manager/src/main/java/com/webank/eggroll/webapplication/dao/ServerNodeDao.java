package com.webank.eggroll.webapplication.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.ServerNode;

import java.util.List;

public class ServerNodeDao {

    @Inject
    ServerNodeService serverNodeService;
    public List<ServerNode> getData(int page, int pageSize) {
        IPage pageStats = new Page();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        List data = this.serverNodeService.list(pageStats);
        return data;
    }
}
