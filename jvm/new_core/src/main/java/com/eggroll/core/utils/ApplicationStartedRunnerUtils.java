package com.eggroll.core.utils;

import com.eggroll.core.postprocessor.ApplicationStartedRunner;
import com.google.inject.Injector;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ApplicationStartedRunnerUtils {

    public static void run(Injector injector, String[] args) throws Exception {
        List<ApplicationStartedRunner> listenerList = getAllImplementations(injector);
        List<ApplicationStartedRunner> sortedList = listenerList.stream().sorted(Comparator.comparingInt(n->n.getRunnerSequenceId())).collect(Collectors.toList());
        for (ApplicationStartedRunner applicationStartedRunner : sortedList) {
            applicationStartedRunner.run(args);
        }
    }

    private static List<ApplicationStartedRunner> getAllImplementations(Injector injector) {
        List<ApplicationStartedRunner> implementations = new ArrayList<>();
        Reflections reflections = new Reflections("com.webank.eggroll");
        Set<Class<? extends ApplicationStartedRunner>> subClasses = reflections.getSubTypesOf(ApplicationStartedRunner.class);
        if (subClasses != null) {
            for (Class<? extends ApplicationStartedRunner> subClass : subClasses) {
                ApplicationStartedRunner subclass = injector.getInstance(subClass);
                if (subclass != null) {
                    implementations.add(subclass);
                }
            }
        }
        return implementations;
    }
}
