package com.webank.eggroll.webapplication.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;

import java.util.ArrayList;
import java.util.List;

public interface ProcessorResourceServiceN {

    List<ProcessorResource> getAllResources();

}