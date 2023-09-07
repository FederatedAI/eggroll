package com.webank.eggroll.webapplication.dao;

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

        IPage pageStats = new Page();
        pageStats.setSize(pageSize);
        pageStats.setCurrent(page);

        List data = this.processorResourceService.list(pageStats);

        return data;
    }

}