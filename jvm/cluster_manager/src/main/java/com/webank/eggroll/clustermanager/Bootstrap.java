package com.webank.eggroll.clustermanager;

import com.eggroll.core.boostrap.CommonBoostrap;
import com.eggroll.core.postprocessor.ApplicationStartedRunnerUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.ClusterModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Bootstrap{

    static Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    static public Injector injector;

    public static void main(String[] args) throws Exception {
        CommonBoostrap.init(args,"cluster-manager");
        injector = Guice.createInjector(new ClusterModule());
        List<String> packages = new ArrayList<>();
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
