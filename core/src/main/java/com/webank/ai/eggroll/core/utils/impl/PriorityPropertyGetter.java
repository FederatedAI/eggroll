package com.webank.ai.eggroll.core.utils.impl;

import com.google.common.collect.Lists;
import com.webank.ai.eggroll.core.utils.PropertyGetter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Scope("prototype")
public class PriorityPropertyGetter implements PropertyGetter {
    private final List<Properties> propertiesPriorityList;

    public PriorityPropertyGetter() {
        this.propertiesPriorityList = Collections.synchronizedList(Lists.newArrayList());
    }

    @Override
    public boolean addSource(Properties prop) {
        return propertiesPriorityList.add(prop);
    }

    @Override
    public List<Properties> getAllSources() {
        return Collections.unmodifiableList(propertiesPriorityList);
    }

    @Override
    public String getProperty(String key) {
        return getProperty(key, (String) null);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return getPropertyInIterable(key, defaultValue, propertiesPriorityList);
    }

    @Override
    public String getPropertyWithTemporarySource(String key, Properties... props) {
        return getPropertyWithTemporarySource(key, null, props);
    }

    @Override
    public String getPropertyWithTemporarySource(String key, String defaultValue, Properties... props) {
        ArrayList<Properties> appended = Lists.newArrayListWithCapacity(props.length + propertiesPriorityList.size());
        appended.addAll(Arrays.asList(props));
        appended.addAll(propertiesPriorityList);
        return getPropertyInIterable(key, defaultValue, appended);
    }

    public String getPropertyInIterable(String key, Iterable<Properties> propsIter) {
        return getPropertyInIterable(key, null, propsIter);
    }

    public String getPropertyInIterable(String key, String defaultValue, Iterable<Properties> propsIter) {
        String result = null;
        if (propsIter == null) {
            return result;
        }

        for (Properties prop : propsIter) {
            if (prop == null) {
                continue;
            }

            result = prop.getProperty(key);
            if (result != null) {
                break;
            }
        }

        if (result == null) {
            result = defaultValue;
        }

        return result;
    }
}
