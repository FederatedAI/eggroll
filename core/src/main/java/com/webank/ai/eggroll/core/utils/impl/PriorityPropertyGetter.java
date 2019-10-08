package com.webank.ai.eggroll.core.utils.impl;

import com.google.common.collect.Lists;
import com.webank.ai.eggroll.core.constant.StringConstants;
import com.webank.ai.eggroll.core.utils.PropertyGetter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;

@Service("propertyGetter")
public class PriorityPropertyGetter implements PropertyGetter {

    private final List<Properties> propertiesPriorityList;
    private static final Logger LOGGER = LogManager.getLogger();

    public PriorityPropertyGetter() {
        this.propertiesPriorityList = Collections.synchronizedList(Lists.newArrayList());
    }

    @PostConstruct
    public void init() {
/*        if (propertiesPriorityList.isEmpty() && serverConf.getProperties() != null) {
            propertiesPriorityList.add(serverConf.getProperties());
        }*/
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
        if (props == null) {
            props = new Properties[0];
        }

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
            if (prop == null || prop.isEmpty()) {
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
