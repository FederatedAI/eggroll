package com.webank.eggroll.webapplication.service;

import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;

import java.util.List;

public class ProcessorResourceService {
    private ProcessorResourceDao resourceDao;

    @Inject
    public ProcessorResourceService(ProcessorResourceDao resourceDao) {
        this.resourceDao = resourceDao;
    }

    public List<ProcessorResource> getAllResources(int page, int size) {
        int offset = (page - 1) * size;
//        return resourceDao.getAllResources(offset, size);
        return null;
    }
}