package com.webank.ai.eggroll.core.utils;

import java.util.List;
import java.util.Properties;

public interface PropertyGetter {
    public boolean addSource(Properties prop);
    public List<Properties> getAllSources();
    public String getProperty(String key);
    public String getProperty(String key, String defaultValue);
    public String getPropertyWithTemporarySource(String key, Properties ... props);
    public String getPropertyWithTemporarySource(String key, String defaultValue, Properties ... props);
}
