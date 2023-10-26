package com.webank.eggroll.nodemanager;

import com.eggroll.core.boostrap.CommonBoostrap;
import com.eggroll.core.postprocessor.ApplicationStartedRunnerUtils;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.NodeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Bootstrap {
    static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) throws Exception {
        CommonBoostrap.init(args, "node-manager");
        Injector injector = Guice.createInjector(new NodeModule());
        List<String> packages = Lists.newArrayList();
        packages.add(Bootstrap.class.getPackage().getName());
        ApplicationStartedRunnerUtils.run(injector, packages, args);
        synchronized (injector) {
            try {
                injector.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
