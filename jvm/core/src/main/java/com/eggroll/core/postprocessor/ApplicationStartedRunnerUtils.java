package com.eggroll.core.postprocessor;

import com.google.inject.Injector;
import org.reflections.Reflections;

import java.util.*;
import java.util.stream.Collectors;

public class ApplicationStartedRunnerUtils {

    public static void run(Injector injector, List<String> packages, String[] args) throws Exception {
        List<ApplicationStartedRunner> listenerList = getAllImplementations(injector, packages);
        List<ApplicationStartedRunner> sortedList = listenerList.stream().sorted(Comparator.comparingInt(ApplicationStartedRunner::getRunnerSequenceId)).collect(Collectors.toList());
        for (ApplicationStartedRunner applicationStartedRunner : sortedList) {
            applicationStartedRunner.run(args);
        }
    }

    private static List<ApplicationStartedRunner> getAllImplementations(Injector injector, List<String> packages) {
        List<ApplicationStartedRunner> implementations = new ArrayList<>();
        Set<Class<? extends ApplicationStartedRunner>> subClasses = new HashSet<>();
        // add core
        String corePackage = "com.eggroll.core";
        if (!packages.contains(corePackage)) {
            packages.add(corePackage);
        }
        for (String scanPackage : packages) {
            Reflections reflections = new Reflections(scanPackage);
            subClasses.addAll(reflections.getSubTypesOf(ApplicationStartedRunner.class));
        }
        for (Class<? extends ApplicationStartedRunner> subClass : subClasses) {
            ApplicationStartedRunner subclass = injector.getInstance(subClass);
            if (subclass != null) {
                implementations.add(subclass);
            }
        }
        return implementations;
    }
}
