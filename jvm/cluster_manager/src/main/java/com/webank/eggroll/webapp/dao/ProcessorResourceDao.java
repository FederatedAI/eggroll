package com.webank.eggroll.webapp.dao;

import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import java.util.List;

public class ProcessorResourceDao {
    @Inject
    ProcessorResourceService processorResourceService;

    public List<ProcessorResource> getData(int page, int pageSize) {
        PageHelper.startPage(page, pageSize);
        return this.processorResourceService.list();
    }
}
