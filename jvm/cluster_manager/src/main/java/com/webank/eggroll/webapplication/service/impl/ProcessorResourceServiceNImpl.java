package com.webank.eggroll.webapplication.service.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;
import com.webank.eggroll.webapplication.service.ProcessorResourceServiceN;


import java.util.List;

@Singleton
public class ProcessorResourceServiceNImpl implements ProcessorResourceServiceN {
    private ProcessorResourceDao resourceDao;

    @Inject
    public ProcessorResourceServiceNImpl(ProcessorResourceDao resourceDao) {
        this.resourceDao = resourceDao;
    }
    @Override
    public List<ProcessorResource> getAllResources() {

        return resourceDao.getData();
    }

}
