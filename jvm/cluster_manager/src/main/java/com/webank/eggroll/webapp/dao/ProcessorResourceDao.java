package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import java.util.List;

public class ProcessorResourceDao {
    @Inject
    ProcessorResourceService processorResourceService;

    public List<ProcessorResource> getData(int page, int pageSize) {

        IPage<ProcessorResource> pageStats = new Page<>();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);
        return this.processorResourceService.list(pageStats);
    }

}
