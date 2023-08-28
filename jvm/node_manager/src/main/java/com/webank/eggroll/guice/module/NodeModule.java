package com.webank.eggroll.guice.module;

import com.google.inject.AbstractModule;
import com.webank.eggroll.nodemanager.grpc.GrpcServer;

public class NodeModule extends AbstractModule {

    @Override
    protected void configure() {
        super.configure();

//       bind(GrpcServer.class);
    }
}
