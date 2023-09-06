package com.webank.eggroll.webapplication.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.entity.NodeResource;

import java.util.List;

@Singleton
public class NodeResourceDao {

    @Inject
    NodeResourceService nodeResourceService;
    public List<NodeResource> getData(int page, int pageSize) {
        IPage pageStats = new Page();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        List data = this.nodeResourceService.list(pageStats);
        return data;
    }
}
