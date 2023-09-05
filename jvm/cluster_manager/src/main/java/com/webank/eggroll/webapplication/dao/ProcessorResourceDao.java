package com.webank.eggroll.webapplication.dao;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import java.util.ArrayList;
import java.util.List;

public class ProcessorResourceDao {
//    @Inject
//    ProcessorResourceService processorResourceService;
//    @Inject
//    public ProcessorResourceDao(ProcessorResourceService processorResourceService) {
//        this.processorResourceService = processorResourceService;
//    }

    public List<ProcessorResource> getData() {

//        List<ProcessorResource> list = this.processorResourceService.list();
        ProcessorResource test = new ProcessorResource();
        List<ProcessorResource> lsResource = new ArrayList<>();
        test.setId(10001L);
        test.setStatus("测试状态");
        lsResource.add(test);
        return lsResource;
    }

}
//        ProcessorResource test = new ProcessorResource();
//        List<ProcessorResource> lsResource = new ArrayList<>();
//        test.setId(10001L);
//        test.setStatus("测试状态");
//        lsResource.add(test);