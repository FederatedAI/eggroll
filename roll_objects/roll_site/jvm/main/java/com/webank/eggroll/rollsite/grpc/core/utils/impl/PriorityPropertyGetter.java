package com.webank.eggroll.rollsite.grpc.core.utils.impl;

import com.google.common.collect.Lists;
import com.webank.eggroll.rollsite.grpc.core.constant.StringConstants;
import com.webank.eggroll.rollsite.grpc.core.utils.PropertyGetter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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

    public String getAllMatchingPropertiesInIterable(CharSequence delimiter, String key, Iterable<Properties> propsIter) {
        return getPropertyInIterableInternal(delimiter, key, null, propsIter, -1);
    }

    public String getAllMatchingPropertiesInIterable(CharSequence delimiter, String key, String defaultValue, Iterable<Properties> propsIter) {
        return getPropertyInIterableInternal(delimiter, key, defaultValue, propsIter, -1);
    }

    public String getPropertyInIterable(String key, Iterable<Properties> propsIter) {
        return getPropertyInIterable(key, null, propsIter);
    }

    public String getPropertyInIterable(String key, String defaultValue, Iterable<Properties> propsIter) {
        /*String result = null;
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

        return result;*/

        return getPropertyInIterableInternal(StringConstants.EMPTY, key, defaultValue, propsIter, 1);
    }

    public String getPropertyInIterableInternal(CharSequence delimiter, String key, String defaultValue, Iterable<Properties> propsIter, int maxMatch) {
        String result = defaultValue;
        List<String> matchedValues = Lists.newLinkedList();

        if (propsIter == null) {
            return result;
        }

        int curMatch = 0;
        String value;
        for (Properties prop : propsIter) {
            if (prop == null) {
                continue;
            }

            value = prop.getProperty(key);
            if (StringUtils.isNotBlank(value)) {
                matchedValues.add(value);
                if (maxMatch > 0 && ++curMatch >= maxMatch) {
                    break;
                }
            }
        }

        if (!matchedValues.isEmpty()) {
            result = String.join(delimiter, matchedValues);
        }

        return result;
    }
}
