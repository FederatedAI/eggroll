package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import java.util.List;

@Singleton
public class NodeResourceDao {

    @Inject
    NodeResourceService nodeResourceService;
    public List<NodeResource> getData(int page, int pageSize) {
        PageHelper.startPage(page, pageSize);
        return this.nodeResourceService.list();
    }
}
