package com.webank.eggroll.clustermanager.processor;

import com.eggroll.core.postprocessor.ApplicationStartedListener;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.reflections.Reflections;

import java.util.*;
import java.util.stream.Collectors;

public class ApplicationStartedRunner {

    public static void run(Injector injector, String[] args) throws Exception {
        Map<Key<?>, Binding<?>> allBindings = injector.getAllBindings();
        List<ApplicationStartedListener> listenerList = getAllImplementations(injector);
        List<ApplicationStartedListener> sortedList = listenerList.stream().sorted(Comparator.comparingInt(ApplicationStartedListener::getSequenceId)).collect(Collectors.toList());
        for (ApplicationStartedListener applicationStartedListener : sortedList) {
            applicationStartedListener.onApplicationStarted(args);
        }
    }

    private static List<ApplicationStartedListener> getAllImplementations(Injector injector) {
        List<ApplicationStartedListener> implementations = new ArrayList<>();
        Reflections reflections = new Reflections("com.webank.eggroll");

        Set<Class<? extends ApplicationStartedListener>> subClasses = reflections.getSubTypesOf(ApplicationStartedListener.class);
        if (subClasses != null) {
            for (Class<? extends ApplicationStartedListener> subClass : subClasses) {
                ApplicationStartedListener subclass = injector.getInstance(subClass);
                if (subclass != null) {
                    implementations.add(subclass);
                }
            }
        }
        return implementations;
    }
}
